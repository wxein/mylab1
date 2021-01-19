package com.dbs.celerity.queryrunner.service.metadata;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.dbs.celerity.queryrunner.model.S3MetaData;
import com.dbs.celerity.queryrunner.model.UserProfile;
import com.dbs.celerity.queryrunner.userdata.entity.S3QueryPermission;
import com.dbs.celerity.queryrunner.userdata.repo.S3QueryPermissionRepo;
import com.dbs.celerity.queryrunner.webauth.UserRequestContext;
import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicStringProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Objects.isNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Service that processes REST API requests for Metadata information.
 */
@Service
public class S3MetaDataService {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3MetaDataService.class);

    private static final DynamicStringProperty ENDPOINT = new DynamicStringProperty("s3.endpoint", null);
    private static final DynamicStringProperty REGION = new DynamicStringProperty("s3.region", null);
    private static final DynamicIntProperty MAX_RETRIES = new DynamicIntProperty("s3.max-retries", 5);
    private static final DynamicIntProperty RELOAD_MINUTES = new DynamicIntProperty("s3.reload.minutes", 60);
    private static final DynamicBooleanProperty CLEAR_METADATA = new DynamicBooleanProperty("s3.clear.metadata.trigger", false);

    private Map<String, AmazonS3> bucketClients;

    private final S3QueryPermissionRepo s3QueryPermissionRepo;
    private final AtomicReference<Map<String, S3MetaData>> metaDataRef;
    private final ScheduledExecutorService executorService;
    private ScheduledFuture<?> scheduledFuture;

    private final ConcurrentMap<String, CacheUserMetadata> userMetadataCache = new ConcurrentHashMap<>();

    @Autowired
    public S3MetaDataService(S3QueryPermissionRepo s3QueryPermissionRepo) {
        this.s3QueryPermissionRepo = s3QueryPermissionRepo;
        this.metaDataRef = new AtomicReference<>(new HashMap());
        this.bucketClients = new HashMap<>();
        this.executorService = newSingleThreadScheduledExecutor();
        this.scheduledFuture = executorService.scheduleAtFixedRate(this::clearMetadata, 0, RELOAD_MINUTES.get(), MINUTES);
        addCallbacks();
    }

    /**
     * Gets metadata information for current user.
     *
     * @return Map of accessible tables, grouped by database, then by datasource type.
     */
    public List<S3MetaData> getMetadata() {
        UserProfile userProfile = UserRequestContext.getOrThrow();
        String userId = userProfile.getUserId();
        userMetadataCache.remove(userId);

        return getUserMetadata().map(e -> new ArrayList<>(e.userMetadata.values())).orElse(new ArrayList<>());
    }

    public Set<String> getUserBuckets() {
        return getUserMetadata().map(e -> e.allowedBuckets).orElse(Collections.emptySet());
    }

    public Set<String> getShareableBuckets() {
        UserProfile userProfile = UserRequestContext.getOrThrow();
        return s3QueryPermissionRepo.findByShareableAndGroupIn(1, userProfile.getGroups()).stream().map(
                e -> e.getBucket().toLowerCase()).collect(Collectors.toSet());
    }

    public AmazonS3 getClient(String bucket) {
        return bucketClients.get(bucket);
    }

    /**
     * Retrieves metadata of current user.
     *
     * @return Metadata of current user, if available.
     */
    private Optional<CacheUserMetadata> getUserMetadata() {
        UserProfile userProfile = UserRequestContext.getOrThrow();
        String userId = userProfile.getUserId();
        if(!userMetadataCache.containsKey(userId)) {
            LOGGER.info("Initializing metadata for user: {}", userId);
            initUserMetadata(userProfile);
        }
        return Optional.ofNullable(userMetadataCache.get(userId));
    }

    /**
     * Initializes metadata for user.
     *
     * @param userProfile UserProfile object of current user.
     */
    private void initUserMetadata(UserProfile userProfile) {
        Map<String, S3MetaData> allMetadata = metaDataRef.get();
        Map<String, S3MetaData> userMetadata = new HashMap<>();

        CacheUserMetadata cacheEntry = new CacheUserMetadata();

        List<S3QueryPermission> permissions = s3QueryPermissionRepo.findByGroupInOrderByBucketAsc(userProfile.getGroups());
        cacheEntry.allowedBuckets = permissions.stream().map(permission -> permission.getBucket().toLowerCase()).collect(Collectors.toSet());

        for(S3QueryPermission permission : permissions) {
            String bucket = permission.getBucket();

            if(!bucketClients.containsKey(bucket)) {
                bucketClients.put(bucket, createClient(permission.getAccessId(), permission.getAccessKey()));
            }

            if(allMetadata.containsKey(bucket)) {
                userMetadata.put(bucket, allMetadata.get(bucket));
            } else {
                S3MetaData bucketMetadata = getBucketMetadata(bucket);

                userMetadata.put(bucket, bucketMetadata);
                allMetadata.put(bucket, bucketMetadata);
            }
        }
        cacheEntry.userMetadata = userMetadata;
        userMetadataCache.put(userProfile.getUserId(), cacheEntry);

        metaDataRef.set(allMetadata);
    }

    private void clearMetadata() {
        this.metaDataRef.set(new HashMap<>());
        this.bucketClients.clear();
    }

    private void addCallbacks() {
        RELOAD_MINUTES.addCallback(() -> {
            LOGGER.info("{} changed to {}! rescheduling metadata reload task", RELOAD_MINUTES.getName(), RELOAD_MINUTES.get());
            scheduledFuture.cancel(false);
            scheduledFuture = executorService.scheduleAtFixedRate(this::clearMetadata, 0, RELOAD_MINUTES.get(), MINUTES);
        });
        CLEAR_METADATA.addCallback(() -> {
            LOGGER.info("Clearing user metadata!");
            clearMetadata();
        });
    }

    private AmazonS3 createClient(String accessId, String accessKey) {
        String endpoint = ENDPOINT.get();
        String region = REGION.get();
        int maxRetries = MAX_RETRIES.get();
        ClientConfiguration clientConfig = new ClientConfiguration().withMaxErrorRetry(maxRetries);
        return AmazonS3Client.builder().withClientConfiguration(clientConfig)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessId, accessKey)))
                .build();
    }

    private S3MetaData getBucketMetadata(String bucket) {
        S3MetaData bucketMetadata = new S3MetaData(bucket);
        AmazonS3 bucketClient = bucketClients.get(bucket);
        ObjectListing objectListing = null;

        do {
            if(isNull(objectListing)) {
                objectListing = bucketClient.listObjects(bucket);
            } else {
                objectListing = bucketClient.listNextBatchOfObjects(objectListing);
            }

            List<S3ObjectSummary> listObjects = objectListing.getObjectSummaries();

            for(S3ObjectSummary object : listObjects) {
                String objectPath = object.getKey();
                buildTree(objectPath, bucketMetadata);
            }
        } while(objectListing.isTruncated());

        return bucketMetadata;
    }

    private void buildTree(String objectPath, S3MetaData bucketData) {
        String[] nodes = objectPath.split("/");

        S3MetaData parent = bucketData;

        for(int i = 0; i < nodes.length; i++) {
            boolean childExists = false;
            String nodeName = nodes[i];

            List<S3MetaData> children = parent.getChildren();
            for(S3MetaData child : children) {
                if(child.getName().equals(nodeName)) {
                    parent = child;
                    childExists = true;
                    break;
                }
            }

            if(!childExists) {
                S3MetaData currentNode = new S3MetaData(nodeName);

                if(i == (nodes.length - 1)) {
                    currentNode.setIsFile(true);
                    currentNode.setBucket(bucketData.getName());
                    currentNode.setFilePath(objectPath);
                }

                parent = parent.addChild(currentNode);
            }
        }
    }

    private static class CacheUserMetadata {
        Map<String, S3MetaData> userMetadata;
        Set<String> allowedBuckets;
    }
}
