package com.spectralogic.ds3client.integration;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.spectrads3.CancelJobSpectraS3Request;
import com.spectralogic.ds3client.commands.spectrads3.CancelJobSpectraS3Response;
import com.spectralogic.ds3client.commands.spectrads3.GetJobChunksReadyForClientProcessingSpectraS3Request;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.integration.test.helpers.TempStorageIds;
import com.spectralogic.ds3client.integration.test.helpers.TempStorageUtil;
import com.spectralogic.ds3client.models.ChecksumType;
import com.spectralogic.ds3client.models.Contents;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.networking.FailedRequestException;
import com.spectralogic.ds3client.serializer.XmlProcessingException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.security.SignatureException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.*;

public class Regression_Test {

    private static final Logger LOG = LoggerFactory.getLogger(Regression_Test.class);

    private static Ds3Client client;
    private static final String TEST_ENV_NAME = "regression_test";
    private static TempStorageIds envStorageIds;

    @BeforeClass
    public static void startup() throws IOException, SignatureException {
        client = Util.fromEnv();
        final UUID dataPolicyId = TempStorageUtil.setupDataPolicy(TEST_ENV_NAME, false, ChecksumType.Type.MD5, client);
        envStorageIds = TempStorageUtil.setup(TEST_ENV_NAME, dataPolicyId, client);
    }

    @AfterClass
    public static void teardown() throws IOException, SignatureException {
        TempStorageUtil.teardown(TEST_ENV_NAME, envStorageIds, client);
        client.close();
    }

    @Test
    public void testMarkerWithSpaces() throws IOException, SignatureException, XmlProcessingException {
        final String bucketName = "marker_with_spaces";
        final Ds3ClientHelpers helpers = Ds3ClientHelpers.wrap(client);

        try {
            helpers.ensureBucketExists(bucketName);

            final List<Ds3Object> objects = Lists.newArrayList(
                    new Ds3Object("obj1_no_spaces.txt", 1024),
                    new Ds3Object("obj2_no_spaces.txt", 1024),
                    new Ds3Object("obj3 has spaces.txt", 1024),
                    new Ds3Object("obj4 also has spaces.txt", 1024));

            final Ds3ClientHelpers.Job putJob = helpers.startWriteJob(bucketName, objects);

            final Iterable<Contents> objs = helpers.listObjects(bucketName, null, "obj3 has spaces.txt");
            boolean foundObj4 = false;
            for (final Contents obj : objs) {
                LOG.info("marker with spaces name: " + obj.getKey());
                if (obj.getKey().equals("obj4 also has spaces.txt")) foundObj4 = true;
                LOG.info("marker with spaces size: " + obj.getSize());
            }
            assertTrue(Iterables.size(objs) == 1);
            assertTrue(foundObj4);

            final CancelJobSpectraS3Response cancelJobResponse = client
                    .cancelJobSpectraS3(new CancelJobSpectraS3Request(putJob.getJobId()));
            assertEquals(204, cancelJobResponse.getStatusCode());
        } finally {
            Util.deleteAllContents(client, bucketName);
        }
    }

    @Test
    public void testPrefixWithSpaces() throws IOException, SignatureException, XmlProcessingException {
        final String bucketName = "prefix_with_spaces";
        final Ds3ClientHelpers helpers = Ds3ClientHelpers.wrap(client);

        try {
            helpers.ensureBucketExists(bucketName);

            final List<Ds3Object> objects = Lists.newArrayList(
                    new Ds3Object("obj1_no_spaces.txt", 1024),
                    new Ds3Object("has spaces obj2.txt", 1024),
                    new Ds3Object("obj3_no_spaces.txt", 1024),
                    new Ds3Object("has spaces obj4.txt", 1024));

            final Ds3ClientHelpers.Job putJob = helpers.startWriteJob(bucketName, objects);

            final Iterable<Contents> objs = helpers.listObjects(bucketName, "has spaces");
            boolean foundObj2 = false;
            boolean foundObj4 = false;
            for (final Contents obj : objs) {
                LOG.info("prefix with spaces name: " + obj.getKey());
                LOG.info("prefix with spaces size: " + obj.getSize());
                if (obj.getKey().equals("has spaces obj2.txt")) foundObj2 = true;
                if (obj.getKey().equals("has spaces obj4.txt")) foundObj4 = true;
            }
            assertTrue(Iterables.size(objs) == 2);
            assertTrue(foundObj2);
            assertTrue(foundObj4);

            final CancelJobSpectraS3Response cancelJobResponse = client
                    .cancelJobSpectraS3(new CancelJobSpectraS3Request(putJob.getJobId()));
            assertEquals(204, cancelJobResponse.getStatusCode());
        } finally {
            Util.deleteAllContents(client, bucketName);
        }
    }

    @Test
    public void testPrefixForDirectoriesWithSpaces() throws IOException, SignatureException, XmlProcessingException {
        final String bucketName = "prefix_directory_with_spaces";
        final Ds3ClientHelpers helpers = Ds3ClientHelpers.wrap(client);

        try {
            helpers.ensureBucketExists(bucketName);

            final List<Ds3Object> objects = Lists.newArrayList(
                    new Ds3Object("dir1/obj1_no_spaces.txt", 1024),
                    new Ds3Object("dir1/has spaces obj2.txt", 1024),
                    new Ds3Object("dir 2/obj3_no_spaces.txt", 1024),
                    new Ds3Object("dir 2/has spaces obj4.txt", 1024));

            final Ds3ClientHelpers.Job putJob = helpers.startWriteJob(bucketName, objects);

            final Iterable<Contents> dir1Objs = helpers.listObjects(bucketName, "dir1/");
            boolean foundObj1 = false;
            boolean foundObj2 = false;
            for (final Contents obj : dir1Objs) {
                LOG.info("prefix with spaces name: " + obj.getKey());
                LOG.info("prefix with spaces size: " + obj.getSize());
                if (obj.getKey().equals("dir1/obj1_no_spaces.txt")) foundObj1 = true;
                if (obj.getKey().equals("dir1/has spaces obj2.txt")) foundObj2 = true;
            }
            assertTrue(Iterables.size(dir1Objs) == 2);
            assertTrue(foundObj1);
            assertTrue(foundObj2);

            final Iterable<Contents> objsDir2 = helpers.listObjects(bucketName, "dir 2/");
            boolean foundObj3 = false;
            boolean foundObj4 = false;
            for (final Contents obj : objsDir2) {
                LOG.info("prefix with spaces name: " + obj.getKey());
                LOG.info("prefix with spaces size: " + obj.getSize());
                if (obj.getKey().equals("dir 2/obj3_no_spaces.txt")) foundObj3 = true;
                if (obj.getKey().equals("dir 2/has spaces obj4.txt")) foundObj4 = true;
            }
            assertTrue(Iterables.size(objsDir2) == 2);
            assertTrue(foundObj3);
            assertTrue(foundObj4);

            final CancelJobSpectraS3Response cancelJobResponse = client
                    .cancelJobSpectraS3(new CancelJobSpectraS3Request(putJob.getJobId()));
            assertEquals(204, cancelJobResponse.getStatusCode());
        } finally {
            Util.deleteAllContents(client, bucketName);
        }
    }

    @Test
    public void testPrefixForNestedDirectories() throws IOException, SignatureException, XmlProcessingException {
        final String bucketName = "prefix__nested_directory";
        final Ds3ClientHelpers helpers = Ds3ClientHelpers.wrap(client);

        try {
            helpers.ensureBucketExists(bucketName);

            final List<Ds3Object> objects = Lists.newArrayList(
                    new Ds3Object("dir1/obj1_no_spaces.txt", 1024),
                    new Ds3Object("dir1/has spaces obj2.txt", 1024),
                    new Ds3Object("dir1/dir 2/obj3_no_spaces.txt", 1024),
                    new Ds3Object("dir1/dir 2/has spaces obj4.txt", 1024));

            final Ds3ClientHelpers.Job putJob = helpers.startWriteJob(bucketName, objects);

            final Iterable<Contents> dir1Objs = helpers.listObjects(bucketName, "dir1/");
            boolean foundObj1 = false;
            boolean foundObj2 = false;
            for (final Contents obj : dir1Objs) {
                LOG.info("prefix with spaces name: " + obj.getKey());
                LOG.info("prefix with spaces size: " + obj.getSize());
                if (obj.getKey().equals("dir1/obj1_no_spaces.txt")) foundObj1 = true;
                if (obj.getKey().equals("dir1/has spaces obj2.txt")) foundObj2 = true;
            }
            assertTrue(Iterables.size(dir1Objs) == 4);
            assertTrue(foundObj1);
            assertTrue(foundObj2);

            final Iterable<Contents> objsDir2 = helpers.listObjects(bucketName, "dir1/dir 2/");
            boolean foundObj3 = false;
            boolean foundObj4 = false;
            for (final Contents obj : objsDir2) {
                LOG.info("prefix with spaces name: " + obj.getKey());
                LOG.info("prefix with spaces size: " + obj.getSize());
                if (obj.getKey().equals("dir1/dir 2/obj3_no_spaces.txt")) foundObj3 = true;
                if (obj.getKey().equals("dir1/dir 2/has spaces obj4.txt")) foundObj4 = true;
            }
            assertTrue(Iterables.size(objsDir2) == 2);
            assertTrue(foundObj3);
            assertTrue(foundObj4);

            final CancelJobSpectraS3Response cancelJobResponse = client
                    .cancelJobSpectraS3(new CancelJobSpectraS3Request(putJob.getJobId()));
            assertEquals(204, cancelJobResponse.getStatusCode());
        } finally {
            Util.deleteAllContents(client, bucketName);
        }
    }

    @Test
    public void emptyObjectTest() throws IOException, SignatureException, XmlProcessingException {
        Util.assumeVersion1_2(client);
        final String bucketName = "emptyObject";
        final List<Ds3Object> objects = Collections.singletonList(new Ds3Object("obj1.txt", 0));

        try {

            final Ds3ClientHelpers helpers = Ds3ClientHelpers.wrap(client);

            helpers.ensureBucketExists(bucketName);

            final Ds3ClientHelpers.Job job = helpers.startWriteJob(bucketName, objects);

            assertThat(job, is(notNullValue()));

            try {
                client.getJobChunksReadyForClientProcessingSpectraS3(
                        new GetJobChunksReadyForClientProcessingSpectraS3Request(job.getJobId()));
                fail();
            } catch(final FailedRequestException e) {
                assertThat(e.getStatusCode(), is(404)); // this returns 410 in bp 3.0
            }

            job.transfer(new Ds3ClientHelpers.ObjectChannelBuilder() {
                @Override
                public SeekableByteChannel buildChannel(final String key) throws IOException {
                    fail("This call should never be hit");
                    return new NullChannel();
                }
            });
        } finally {
            Util.deleteAllContents(client, bucketName);
        }

    }
}
