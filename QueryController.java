package com.dbs.celerity.queryrunner.api.query;

import com.dbs.celerity.queryrunner.model.MetaData;
import com.dbs.celerity.queryrunner.service.metadata.MetaDataService;
import com.dbs.celerity.queryrunner.service.query.QueryRunnerService;
import com.dbs.celerity.queryrunner.userdata.dto.QueryDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.springframework.http.ResponseEntity.ok;

/**
 * Controls API called from Query Runner.
 */
@RestController
@RequestMapping("/query")
public class QueryController {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryController.class);

    private final MetaDataService metaDataService;
    private final QueryRunnerService queryRunnerService;

    @Autowired
    public QueryController(MetaDataService metaDataService, QueryRunnerService queryRunnerService) {
        this.metaDataService = metaDataService;
        this.queryRunnerService = queryRunnerService;
    }

    /**
     * REST API called when retrieving list of databases, grouped by query type.
     *
     * @return Set of databases, pushed into List by query type.
     */
    @GetMapping("/databases")
    public List<Set<String>> getDatabases() {
        LOGGER.info("Processing query databases request ...");
        return metaDataService.getDatabases();
    }

    /**
     * REST API called when retrieving user metadata.
     *
     * @return Map of accessible tables, grouped by database, then by datasource type.
     */
    @GetMapping("/metadata")
    public Map<String, Map<String, MetaData>> getMetadata() {
        LOGGER.info("Processing query metadata request ...");
        return metaDataService.getMetadata();
    }

    /**
     * REST API called when retrieving list of fields in a specific table.
     *
     * @param ds    DataSource type (Impala, Hive, MariaDB, etc...).
     * @param db    database where table is found in.
     * @param table table to retrieve list of fields for.
     * @return Set of table fields.
     */
    @GetMapping(path = "/metadata/{ds}/{db}/{table}", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public Set<String> getTableFields(@PathVariable String ds, @PathVariable String db, @PathVariable String table) {
        LOGGER.info("Processing query table fields request {}.{} in {} ...", db, table, ds);
        return metaDataService.getTableFields(ds, db, table);
    }

    /**
     * REST API called when retrieving list of available query types (Impala, Hive, MariaDB, etc...).
     *
     * @return Set of available query types.
     */
    @GetMapping("/types")
    public Set<String> getQueryTypes() {
        LOGGER.info("Processing query query types request ...");
        return metaDataService.getQueryTypes();
    }

    /**
     * REST API called when user attempts to execute a query.
     *
     * @param request Query object containing details of user's query.
     * @return Query object with updated details from query backend.
     */
    @PostMapping("/execute")
    public QueryDto executeQuery(@RequestBody QueryDto request) {
        LOGGER.info("Processing query execute request {} ...", request.toString());
        return queryRunnerService.executeQuery(request);
    }

    /**
     * REST API called when details of specific query is needed.
     *
     * @param id Primary key ID of query to be retrieved.
     * @return ResponseEntity containing details of retrieved query.
     */
    @GetMapping(path = "/query/{id}", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<QueryDto> getQuery(@PathVariable long id) {
        LOGGER.info("Processing query get request {} ...", id);
        return queryRunnerService.getQuery(id);
    }

    @GetMapping(path = "/queryCount", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public long getQueryCount() {
        LOGGER.info("Processing query get count request ...");
        return queryRunnerService.getQueryCount();
    }

    /**
     * REST API called to retrieve user's query history.
     *
     * @return List of 20 most recent queries by the user.
     */
    @GetMapping(path = "/queries", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public List<QueryDto> getQueries(@RequestParam int pageNumber) {
        LOGGER.info("Processing query get queries request ...");
        return queryRunnerService.getQueries(pageNumber);
    }

    /**
     * REST API called when attempting to request cancellation of query.
     *
     * @param id Primary key ID of query to be cancelled.
     * @return Updated details of query to be cancelled.
     */
    @GetMapping("/cancel/{id}")
    public QueryDto cancelQuery(@PathVariable long id) {
        LOGGER.info("Processing query cancel request {} ...", id);
        return queryRunnerService.cancelQuery(id);
    }

    /**
     * REST API called when deleting a query from history.
     *
     * @param id Primary key ID of query to be deleted.
     * @return Code 200(OK) response of deletion success, along with details of deleted query.
     */
    @DeleteMapping(path = "/delete/{id}", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<Map<String, Object>> deleteQuery(@PathVariable long id) {
        LOGGER.info("Processing query delete from history request {} ...", id);
        return ok(Collections.singletonMap("success", queryRunnerService.deleteQuery(id)));
    }

    /**
     * REST API called when attempting to retrieve results of successfully executed query.
     *
     * @param id Primary key ID of query to retrieve results for.
     * @return Code 200(OK), with stream resource of result file if available,
     * otherwise Code 404(NOT FOUND).
     */
    @GetMapping(path = "/result/{id}", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<InputStreamResource> getQueryResult(@PathVariable long id) {
        LOGGER.info("Processing query get result request {} ...", id);
        return queryRunnerService.getQueryResult(id);
    }

    /**
     * REST API called when attempting to download results of successfully executed query.
     *
     * @param id Primary key ID of query to download results for.
     * @return Code 200(OK), with system resource of result file if available,
     * otherwise Code 404(NOT FOUND).
     */
    @GetMapping(path = "/download/{id}", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<FileSystemResource> downloadQueryResult(@PathVariable long id) {
        LOGGER.info("Processing query download request {} ...", id);
        return queryRunnerService.downloadQueryResult(id);
    }
}
