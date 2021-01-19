@RestController
@RequestMapping("/s3query")
public class S3QueryController {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3QueryController.class);

    private final S3QueryRepo s3QueryRepo;
    private final S3MetaDataService s3MetaDataService;
    private final S3QueryService s3QueryService;
    private final EntityDtoConverter entityDtoConverter;

    @Autowired
    public S3QueryController(S3QueryRepo s3QueryRepo, S3QueryService s3QueryService, S3MetaDataService s3MetaDataService,
                             EntityDtoConverter entityDtoConverter) {
        this.s3QueryRepo = s3QueryRepo;
        this.s3QueryService = s3QueryService;
        this.s3MetaDataService = s3MetaDataService;
        this.entityDtoConverter = entityDtoConverter;
    }

    @GetMapping("/metadata")
    public List<S3MetaData> getMetadata() {
        LOGGER.info("Processing S3 metadata request ...");
        return s3MetaDataService.getMetadata();
    }

    @GetMapping("/buckets")
    public Set<String> getBuckets() {
        LOGGER.info("Processing S3 buckets request ...");
        return s3MetaDataService.getUserBuckets();
    }

    @GetMapping(path = "/query/{id}", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<S3QueryDto> getQuery(@PathVariable long id) {
        LOGGER.info("Processing query get request {} ...", id);
        Optional<S3Query> query = s3QueryRepo.findOneByIdAndUserId(id, UserRequestContext.getUserId());

        if(query.isPresent()) {
            S3QueryDto queryDto = entityDtoConverter.convertToS3QueryDto(query.get());
            return ok(queryDto);
        } else {
            return notFound().build();
        }
    }

    @GetMapping(path = "/queries", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public List<S3QueryDto> getQueries() {
        LOGGER.info("Processing query get queries request ...");
        List<S3Query> queries = s3QueryRepo.findFirst20ByUserIdOrderByIdDesc(UserRequestContext.getUserId());
        return queries.stream().map(entityDtoConverter::convertToS3QueryDto).collect(Collectors.toList());
    }

    @GetMapping(path = "/shareableBuckets", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public Set<String> getShareableBuckets() {
        LOGGER.info("Processing s3 get shareable buckets request ...");
        return s3MetaDataService.getShareableBuckets();
    }

    @PostMapping("/execute")
    public S3QueryDto executeQuery(@RequestBody S3QueryDto request) {
        LOGGER.info("Processing query execute request {} ...", request.toString());
        S3Query queryRequest = entityDtoConverter.convertToS3QueryEntity(request);
        S3Query query = s3QueryService.executeQuery(queryRequest);
        return entityDtoConverter.convertToS3QueryDto(query);
    }

    @GetMapping("/cancel/{id}")
    public S3QueryDto cancelQuery(@PathVariable long id) {
        LOGGER.info("Processing query cancel request {} ...", id);
        S3Query query = s3QueryService.cancelQuery(id);
        return entityDtoConverter.convertToS3QueryDto(query);
    }

    @DeleteMapping(path = "/delete/{id}", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<Map<String, Object>> deleteQuery(@PathVariable long id) {
        LOGGER.info("Processing query delete from history request {} ...", id);
        return ok(Collections.singletonMap("success", s3QueryService.deleteQuery(id)));
    }

    @GetMapping(path = "/result/{id}", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<InputStreamResource> getQueryResult(@PathVariable long id) {
        LOGGER.info("Processing query get result request {} ...", id);
        InputStreamResource stream = s3QueryService.getResultFile(id);

        if(stream != null) {
            return ok(stream);
        } else {
            return notFound().build();
        }
    }

    @GetMapping(path = "/download/{id}", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<FileSystemResource> downloadFile(@PathVariable long id) {
        LOGGER.info("Processing file download request {} ...", id);
        FileSystemResource file = s3QueryService.downloadResultFile(id);

        if(file != null) {
            String filename = file.getFilename().replaceAll("[\\\\/:*?\"<>|]", "_");
            HttpHeaders responseHeaders = new HttpHeaders();
            responseHeaders.set("Content-Disposition", "attachment; filename=" + filename);
            return ok().headers(responseHeaders).body(file);
        } else {
            return notFound().build();
        }
    }

    @PostMapping(path = "/downloadFromS3")
    public S3QueryDto downloadS3File(@RequestBody S3QueryDto downloadRequest) {
        LOGGER.info("Processing S3 download request {}...", downloadRequest.toString());
        S3Query request = entityDtoConverter.convertToS3QueryEntity(downloadRequest);
        S3Query response = s3QueryService.downloadS3File(request);
        return entityDtoConverter.convertToS3QueryDto(response);
    }
}
