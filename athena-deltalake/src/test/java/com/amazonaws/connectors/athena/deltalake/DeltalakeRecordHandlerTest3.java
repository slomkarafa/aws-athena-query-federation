/*-
 * #%L
 * athena-deltalake
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.connectors.athena.deltalake;

import com.amazonaws.athena.connector.lambda.data.*;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.connectors.athena.deltalake.converter.DeltaConverter;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.amazonaws.connectors.athena.deltalake.DeltalakeMetadataHandler.SPLIT_FILE_PROPERTY;
import static com.amazonaws.connectors.athena.deltalake.DeltalakeMetadataHandler.SPLIT_PARTITION_VALUES_PROPERTY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class DeltalakeRecordHandlerTest3 extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(DeltalakeRecordHandlerTest3.class);

    private DeltalakeRecordHandler handler;
    private BlockAllocatorImpl allocator;
    private Schema schemaForRead;
    private S3BlockSpillReader spillReader;

    @Rule
    public TestName testName = new TestName();

    @After
    public void after()
    {
        allocator.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Before
    public void setUp() throws IOException {
        logger.info("{}: enter", testName.getMethodName());

        String dataBucket = "test-bucket-1";

        File schemaStringFile = new File(getClass().getClassLoader().getResource("complex_table_schema.json").getFile());
        String schemaString = FileUtils.readFileToString(schemaStringFile, "UTF-8");
        schemaForRead = DeltaConverter.getArrowSchema(schemaString);

        allocator = new BlockAllocatorImpl();

        Configuration conf = new Configuration();
        conf.set("fs.s3a.endpoint", S3_ENDPOINT);
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.access.key", "NO_NEED");
        conf.set("fs.s3a.secret.key", "NO_NEED");

        AwsClientBuilder.EndpointConfiguration endpoint = new AwsClientBuilder.EndpointConfiguration(S3_ENDPOINT, S3_REGION);
        amazonS3 = AmazonS3ClientBuilder
            .standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(endpoint)
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build();
        this.handler = new DeltalakeRecordHandler(
            amazonS3,
            mock(AWSSecretsManager.class),
            mock(AmazonAthena.class),
            conf,
            dataBucket);

        spillReader = new S3BlockSpillReader(amazonS3, allocator);
    }

    @Test
    public void doReadRecordsNoSpill()
            throws Exception
    {
        String catalogName = "catalog";
        for (int i = 0; i < 2; i++) {
            Map<String, ValueSet> constraintsMap = new HashMap<>();
            String queryId = "queryId-" + System.currentTimeMillis();

            ReadRecordsRequest request = new ReadRecordsRequest(fakeIdentity(),
                catalogName,
                queryId,
                new TableName("test-database-2", "complex-table"),
                schemaForRead,
                Split.newBuilder(makeSpillLocation(queryId, "1234"), null)
                    .add(SPLIT_PARTITION_VALUES_PROPERTY, "{\"tenant\":\"testTenant0\"}")
                    .add(SPLIT_FILE_PROPERTY, "tenant=testTenant0/part-00000-5f547a1b-4a97-45f5-a658-55ab320707ae.c000.snappy.parquet")
                    .build(),
                new Constraints(constraintsMap),
                100_000_000_000L, //100GB don't expect this to spill
                100_000_000_000L
            );

            RecordResponse rawResponse = handler.doReadRecords(allocator, request);
            assertTrue(rawResponse instanceof ReadRecordsResponse);

            ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;

            String expectedRow0 = "[tenant : testTenant0], [testArrayString : {foo,bar}], [testArrayInt : {0,1,2,3}], [testMapString : {[key : foo],[value : bar]}{[key : foo1],[value : bar1]}], [testStruct : {[name : foobar],[id : 0]}], [complexStruct : {[testStruct : {[name : foobar],[id : 4]}],[testArray : {foo,bar}],[testMap : {[key : foo],[value : bar]}{[key : foo1],[value : bar1]}]}], [arrayOfIntArrays : {{0,1,2},{2,3,4}}], [arrayOfStructs : {{[name : foobar],[id : 1]},{[name : foobar1],[id : 2]}}], [arrayOfMaps : {{[key : foo],[value : bar]}{[key : foo1],[value : bar1]},{[key : foo1],[value : bar1]}{[key : foo2],[value : bar3]}}], [mapWithMaps : {[key : boo],[value : {[key : foo],[value : bar]}{[key : foo1],[value : bar1]}]}], [mapWithStructs : {[key : boo],[value : {[name : foobar],[id : 3]}]}], [mapWithArrays : {[key : boo],[value : {foo,bar}]}]";
            String expectedRow1 = "[tenant : testTenant0], [testArrayString : {foo,zar}], [testArrayInt : {0,1,2,3,4}], [testMapString : {[key : foo],[value : zar]}{[key : foo1],[value : zar1]}], [testStruct : {[name : foozar],[id : 10]}], [complexStruct : {[testStruct : {[name : foozar],[id : 14]}],[testArray : {foo,zar}],[testMap : {[key : foo],[value : zar]}{[key : foo1],[value : zar1]}]}], [arrayOfIntArrays : {{10,11,12},{12,13,14}}], [arrayOfStructs : {{[name : foozar],[id : 11]},{[name : foozar1],[id : 12]}}], [arrayOfMaps : {{[key : foo],[value : zar]}{[key : foo1],[value : zar1]},{[key : foo1],[value : zar1]}{[key : foo2],[value : zar3]}}], [mapWithMaps : {[key : boo],[value : {[key : foo],[value : zar]}{[key : foo1],[value : zar1]}]}], [mapWithStructs : {[key : boo],[value : {[name : foozar],[id : 13]}]}], [mapWithArrays : {[key : boo],[value : {foo,zar}]}]";

            assertEquals(expectedRow0, BlockUtils.rowToString(response.getRecords(), 0));
            assertEquals(expectedRow1, BlockUtils.rowToString(response.getRecords(), 1));
            assertEquals(2, response.getRecords().getRowCount());
        }
    }
}
