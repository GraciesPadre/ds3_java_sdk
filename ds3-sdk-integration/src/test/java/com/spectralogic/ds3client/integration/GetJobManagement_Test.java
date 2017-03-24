/*
 * ****************************************************************************
 *    Copyright 2014-2016 Spectra Logic Corporation. All Rights Reserved.
 *    Licensed under the Apache License, Version 2.0 (the "License"). You may not use
 *    this file except in compliance with the License. A copy of the License is located at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file.
 *    This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 *    CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *    specific language governing permissions and limitations under the License.
 *  ****************************************************************************
 */

package com.spectralogic.ds3client.integration;

import com.google.common.collect.Lists;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientImpl;
import com.spectralogic.ds3client.commands.GetObjectRequest;
import com.spectralogic.ds3client.commands.GetObjectResponse;
import com.spectralogic.ds3client.commands.PutObjectRequest;
import com.spectralogic.ds3client.commands.spectrads3.GetJobSpectraS3Request;
import com.spectralogic.ds3client.commands.spectrads3.GetJobSpectraS3Response;
import com.spectralogic.ds3client.helpers.ChecksumListener;
import com.spectralogic.ds3client.helpers.DataTransferredListener;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.FailureEventListener;
import com.spectralogic.ds3client.helpers.FileObjectGetter;
import com.spectralogic.ds3client.helpers.FileObjectPutter;

import com.spectralogic.ds3client.helpers.MetadataReceivedListener;
import com.spectralogic.ds3client.helpers.ObjectCompletedListener;

import com.spectralogic.ds3client.helpers.WaitingForChunksListener;
import com.spectralogic.ds3client.helpers.events.FailureEvent;

import com.spectralogic.ds3client.helpers.options.ReadJobOptions;
import com.spectralogic.ds3client.integration.test.helpers.ABMTestHelper;
import com.spectralogic.ds3client.integration.test.helpers.Ds3ClientShim;
import com.spectralogic.ds3client.integration.test.helpers.Ds3ClientShimFactory;
import com.spectralogic.ds3client.integration.test.helpers.TempStorageIds;
import com.spectralogic.ds3client.integration.test.helpers.TempStorageUtil;
import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.ChecksumType;
import com.spectralogic.ds3client.models.Priority;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.models.bulk.PartialDs3Object;
import com.spectralogic.ds3client.models.common.Range;
import com.spectralogic.ds3client.networking.Metadata;
import com.spectralogic.ds3client.utils.ResourceUtils;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.spectralogic.ds3client.integration.Util.RESOURCE_BASE_NAME;
import static com.spectralogic.ds3client.integration.Util.deleteAllContents;
import static com.spectralogic.ds3client.integration.Util.deleteBucketContents;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class GetJobManagement_Test {

    private static final Logger LOG = LoggerFactory.getLogger(GetJobManagement_Test.class);

    private static final Ds3Client client = Util.fromEnv();
    private static final Ds3ClientHelpers HELPERS = Ds3ClientHelpers.wrap(client);
    private static final String BUCKET_NAME = "Get_Job_Management_Test";
    private static final String TEST_ENV_NAME = "GetJobManagement_Test";
    private static TempStorageIds envStorageIds;
    private static UUID dataPolicyId;

    @BeforeClass
    public static void startup() throws Exception {
        dataPolicyId = TempStorageUtil.setupDataPolicy(TEST_ENV_NAME, false, ChecksumType.Type.MD5, client);
        envStorageIds = TempStorageUtil.setup(TEST_ENV_NAME, dataPolicyId, client);
        setupBucket(dataPolicyId);
    }

    @AfterClass
    public static void teardown() throws IOException {
        try {
            deleteAllContents(client, BUCKET_NAME);
        } finally {
            TempStorageUtil.teardown(TEST_ENV_NAME, envStorageIds, client);
            client.close();
        }
    }

    /**
     * Creates the test bucket with the specified data policy to prevent cascading test failure
     * when there are multiple data policies
     */
    private static void setupBucket(final UUID dataPolicy) {
        try {
            HELPERS.ensureBucketExists(BUCKET_NAME, dataPolicy);
        } catch (final Exception e) {
            LOG.error("Setting up test environment failed: " + e.getMessage());
        }
    }

    @Before
    public void setupForEachTest() throws Exception {
        LOG.info("Setting up before test.");
        putBigFiles();
        putBeowulf();
    }

    @After
    public void teardownForEachtest() throws IOException {
        LOG.info("Tearing down after test.");
        deleteBucketContents(client, BUCKET_NAME);
    }

    private static void putBeowulf() throws Exception {
        final String book1 = "beowulf.txt";
        final Path objPath1 = ResourceUtils.loadFileResource(RESOURCE_BASE_NAME + book1);
        final Ds3Object obj = new Ds3Object(book1, Files.size(objPath1));
        final Ds3ClientHelpers.Job job = HELPERS.startWriteJob(BUCKET_NAME, Lists
                .newArrayList(obj));
        final UUID jobId = job.getJobId();
        final SeekableByteChannel book1Channel = new ResourceObjectPutter(RESOURCE_BASE_NAME).buildChannel(book1);
        client.putObject(new PutObjectRequest(BUCKET_NAME, book1, book1Channel, jobId, 0, Files.size(objPath1)));
        ABMTestHelper.waitForJobCachedSizeToBeMoreThanZero(jobId, client, 20);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void nakedS3Get() throws IOException,
            URISyntaxException, InterruptedException {

        final WritableByteChannel writeChannel = new NullChannel();

        final GetObjectResponse getObjectResponse = client.getObject(
                new GetObjectRequest(BUCKET_NAME, "beowulf.txt", writeChannel));

        assertThat(getObjectResponse.getStatusCode(), is(200));
    }

    @Test
    public void createReadJob() throws IOException, InterruptedException, URISyntaxException {

        final Ds3ClientHelpers.Job readJob = HELPERS.startReadJob(BUCKET_NAME, Lists.newArrayList(
                new Ds3Object("beowulf.txt", 10)));

        final GetJobSpectraS3Response jobSpectraS3Response = client
                .getJobSpectraS3(new GetJobSpectraS3Request(readJob.getJobId()));

        assertThat(jobSpectraS3Response.getStatusCode(), is(200));
    }

    @Test
    public void createReadJobWithBigFile() throws IOException, URISyntaxException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        final String tempPathPrefix = null;
        final Path tempDirectory = Files.createTempDirectory(Paths.get("."), tempPathPrefix);

        try {
            final String DIR_NAME = "largeFiles/";
            final String FILE_NAME = "lesmis-copies.txt";

            final Path objPath = ResourceUtils.loadFileResource(DIR_NAME + FILE_NAME);
            final long bookSize = Files.size(objPath);
            final Ds3Object obj = new Ds3Object(FILE_NAME, bookSize);

            final Ds3ClientShim ds3ClientShim = new Ds3ClientShim((Ds3ClientImpl)client);

            final int maxNumBlockAllocationRetries = 1;
            final int maxNumObjectTransferAttempts = 3;
            final Ds3ClientHelpers ds3ClientHelpers = Ds3ClientHelpers.wrap(ds3ClientShim,
                    maxNumBlockAllocationRetries,
                    maxNumObjectTransferAttempts);

            final Ds3ClientHelpers.Job readJob = ds3ClientHelpers.startReadJob(BUCKET_NAME, Arrays.asList(obj));

            final AtomicBoolean dataTransferredEventReceived = new AtomicBoolean(false);
            final AtomicBoolean objectCompletedEventReceived = new AtomicBoolean(false);
            final AtomicBoolean checksumEventReceived = new AtomicBoolean(false);
            final AtomicBoolean metadataEventReceived = new AtomicBoolean(false);
            final AtomicBoolean waitingForChunksEventReceived = new AtomicBoolean(false);
            final AtomicBoolean failureEventReceived = new AtomicBoolean(false);

            readJob.attachDataTransferredListener(new DataTransferredListener() {
                @Override
                public void dataTransferred(final long size) {
                    dataTransferredEventReceived.set(true);
                    assertEquals(bookSize, size);
                }
            });
            readJob.attachObjectCompletedListener(new ObjectCompletedListener() {
                @Override
                public void objectCompleted(final String name) {
                    objectCompletedEventReceived.set(true);
                }
            });
            readJob.attachChecksumListener(new ChecksumListener() {
                @Override
                public void value(final BulkObject obj, final ChecksumType.Type type, final String checksum) {
                    checksumEventReceived.set(true);
                    assertEquals("0feqCQBgdtmmgGs9pB/Huw==", checksum);
                }
            });
            readJob.attachMetadataReceivedListener(new MetadataReceivedListener() {
                @Override
                public void metadataReceived(final String filename, final Metadata metadata) {
                    metadataEventReceived.set(true);
                }
            });
            readJob.attachWaitingForChunksListener(new WaitingForChunksListener() {
                @Override
                public void waiting(final int secondsToWait) {
                    waitingForChunksEventReceived.set(true);
                }
            });
            readJob.attachFailureEventListener(new FailureEventListener() {
                @Override
                public void onFailure(final FailureEvent failureEvent) {
                    failureEventReceived.set(true);
                }
            });

            final GetJobSpectraS3Response jobSpectraS3Response = ds3ClientShim
                    .getJobSpectraS3(new GetJobSpectraS3Request(readJob.getJobId()));

            assertThat(jobSpectraS3Response.getStatusCode(), is(200));

            readJob.transfer(new FileObjectGetter(tempDirectory));

            final File originalFile = ResourceUtils.loadFileResource(DIR_NAME + FILE_NAME).toFile();
            final File fileCopiedFromBP = Paths.get(tempDirectory.toString(), FILE_NAME).toFile();
            assertTrue(FileUtils.contentEquals(originalFile, fileCopiedFromBP));

            assertTrue(dataTransferredEventReceived.get());
            assertTrue(objectCompletedEventReceived.get());
            assertTrue(checksumEventReceived.get());
            assertTrue(metadataEventReceived.get());
            assertFalse(waitingForChunksEventReceived.get());
            assertTrue(failureEventReceived.get());
        } finally {
            FileUtils.deleteDirectory(tempDirectory.toFile());
        }
    }

    private static void putBigFiles() throws IOException, URISyntaxException {
        final String DIR_NAME = "largeFiles/";
        final String[] FILE_NAMES = new String[] {"lesmis.txt", "lesmis-copies.txt", "GreatExpectations.txt" };

        final Path dirPath = ResourceUtils.loadFileResource(DIR_NAME);

        final List<String> bookTitles = new ArrayList<>();
        final List<Ds3Object> objects = new ArrayList<>();
        for (final String book : FILE_NAMES) {
            final Path objPath = ResourceUtils.loadFileResource(DIR_NAME + book);
            final long bookSize = Files.size(objPath);
            final Ds3Object obj = new Ds3Object(book, bookSize);

            bookTitles.add(book);
            objects.add(obj);
        }

        final int maxNumBlockAllocationRetries = 1;
        final int maxNumObjectTransferAttempts = 3;
        final Ds3ClientHelpers ds3ClientHelpers = Ds3ClientHelpers.wrap(client,
                maxNumBlockAllocationRetries,
                maxNumObjectTransferAttempts);

        final Ds3ClientHelpers.Job writeJob = ds3ClientHelpers.startWriteJob(BUCKET_NAME, objects);
        writeJob.transfer(new FileObjectPutter(dirPath));
    }

    @Test
    public void createReadJobWithPriorityOption() throws IOException,
            InterruptedException, URISyntaxException {

        final Ds3ClientHelpers.Job readJob = HELPERS.startReadJob(BUCKET_NAME, Lists.newArrayList(
                new Ds3Object("beowulf.txt", 10)), ReadJobOptions.create().withPriority(Priority.LOW));
        final GetJobSpectraS3Response jobSpectraS3Response = client
                .getJobSpectraS3(new GetJobSpectraS3Request(readJob.getJobId()));

        assertThat(jobSpectraS3Response.getMasterObjectListResult().getPriority(), is(Priority.LOW));
    }

    @Test
    public void testCreatingStreamedReadJobWithPriorityOption() throws IOException,
            InterruptedException, URISyntaxException {

        final Ds3ClientHelpers.Job readJob = HELPERS.startReadJobUsingStreamedBehavior(BUCKET_NAME, Lists.newArrayList(
                new Ds3Object("beowulf.txt", 10)), ReadJobOptions.create().withPriority(Priority.LOW));
        final GetJobSpectraS3Response jobSpectraS3Response = client
                .getJobSpectraS3(new GetJobSpectraS3Request(readJob.getJobId()));

        assertThat(jobSpectraS3Response.getMasterObjectListResult().getPriority(), is(Priority.LOW));
    }

    @Test
    public void createReadJobWithNameOption() throws IOException,
            URISyntaxException, InterruptedException {

        final Ds3ClientHelpers.Job readJob = HELPERS.startReadJob(BUCKET_NAME, Lists.newArrayList(
                new Ds3Object("beowulf.txt", 10)), ReadJobOptions.create().withName("test_job"));
        final GetJobSpectraS3Response jobSpectraS3Response = client
                .getJobSpectraS3(new GetJobSpectraS3Request(readJob.getJobId()));

        assertThat(jobSpectraS3Response.getMasterObjectListResult().getName(), is("test_job"));
    }

    @Test
    public void testCreatingStreamedReadJobWithNameOption() throws IOException,
            URISyntaxException, InterruptedException {

        final Ds3ClientHelpers.Job readJob = HELPERS.startReadJobUsingStreamedBehavior(BUCKET_NAME, Lists.newArrayList(
                new Ds3Object("beowulf.txt", 10)), ReadJobOptions.create().withName("test_job"));
        final GetJobSpectraS3Response jobSpectraS3Response = client
                .getJobSpectraS3(new GetJobSpectraS3Request(readJob.getJobId()));

        assertThat(jobSpectraS3Response.getMasterObjectListResult().getName(), is("test_job"));
    }

    @Test
    public void createReadJobWithNameAndPriorityOptions() throws IOException,
            URISyntaxException, InterruptedException {

        final Ds3ClientHelpers.Job readJob = HELPERS.startReadJob(BUCKET_NAME, Lists.newArrayList(
                new Ds3Object("beowulf.txt", 10)), ReadJobOptions.create()
                .withName("test_job").withPriority(Priority.LOW));
        final GetJobSpectraS3Response jobSpectraS3Response = client
                .getJobSpectraS3(new GetJobSpectraS3Request(readJob.getJobId()));

        assertThat(jobSpectraS3Response.getMasterObjectListResult().getName(), is("test_job"));
        assertThat(jobSpectraS3Response.getMasterObjectListResult().getPriority(), is(Priority.LOW));
    }

    @Test
    public void testCreatingStreamedReadJobWithNameAndPriorityOption() throws IOException,
            URISyntaxException, InterruptedException {

        final Ds3ClientHelpers.Job readJob = HELPERS.startReadJobUsingStreamedBehavior(BUCKET_NAME, Lists.newArrayList(
                new Ds3Object("beowulf.txt", 10)), ReadJobOptions.create()
                .withName("test_job").withPriority(Priority.LOW));
        final GetJobSpectraS3Response jobSpectraS3Response = client
                .getJobSpectraS3(new GetJobSpectraS3Request(readJob.getJobId()));

        assertThat(jobSpectraS3Response.getMasterObjectListResult().getName(), is("test_job"));
        assertThat(jobSpectraS3Response.getMasterObjectListResult().getPriority(), is(Priority.LOW));
    }

    @Test
    public void testPartialRetriesWithInjectedFailures() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException, URISyntaxException {
        final String tempPathPrefix = null;
        final Path tempDirectory = Files.createTempDirectory(Paths.get("."), tempPathPrefix);

        try {
            final List<Ds3Object> filesToGet = new ArrayList<>();

            final String DIR_NAME = "largeFiles/";
            final String FILE_NAME = "GreatExpectations.txt";

            final int offsetIntoFirstRange = 10;

            filesToGet.add(new PartialDs3Object(FILE_NAME, Range.byLength(200000, 100000)));

            filesToGet.add(new PartialDs3Object(FILE_NAME, Range.byLength(100000, 100000)));

            filesToGet.add(new PartialDs3Object(FILE_NAME, Range.byLength(offsetIntoFirstRange, 100000)));

            final Ds3ClientShim ds3ClientShim = new Ds3ClientShim((Ds3ClientImpl) client);

            final int maxNumBlockAllocationRetries = 1;
            final int maxNumObjectTransferAttempts = 4;
            final Ds3ClientHelpers ds3ClientHelpers = Ds3ClientHelpers.wrap(ds3ClientShim,
                    maxNumBlockAllocationRetries,
                    maxNumObjectTransferAttempts);

            final Ds3ClientHelpers.Job job = ds3ClientHelpers.startReadJob(BUCKET_NAME, filesToGet);
            final AtomicInteger intValue = new AtomicInteger();

            job.attachObjectCompletedListener(new ObjectCompletedListener() {
                int numPartsCompleted = 0;

                @Override
                public void objectCompleted(final String name) {
                    assertEquals(1, ++numPartsCompleted);
                    intValue.incrementAndGet();
                }
            });

            job.attachDataTransferredListener(new DataTransferredListener() {
                @Override
                public void dataTransferred(final long size) {
                    LOG.info("Data transferred size: {}", size);
                }
            });

            job.transfer(new FileObjectGetter(tempDirectory));

            assertEquals(1, intValue.get());

            try (final InputStream originalFileStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(DIR_NAME + FILE_NAME)) {
                final byte[] first300000Bytes = new byte[300000 - offsetIntoFirstRange];
                originalFileStream.skip(offsetIntoFirstRange);
                int numBytesRead = originalFileStream.read(first300000Bytes, 0, 300000 - offsetIntoFirstRange);

                assertThat(numBytesRead, is(300000 -offsetIntoFirstRange ));

                try (final InputStream fileReadFromBP = Files.newInputStream(Paths.get(tempDirectory.toString(), FILE_NAME))) {
                    final byte[] first300000BytesFromBP = new byte[300000 - offsetIntoFirstRange];

                    numBytesRead = fileReadFromBP.read(first300000BytesFromBP, 0, 300000 - offsetIntoFirstRange);
                    assertThat(numBytesRead, is(300000 - offsetIntoFirstRange));

                    assertTrue(Arrays.equals(first300000Bytes, first300000BytesFromBP));
                }
            }
        } finally {
            FileUtils.deleteDirectory(tempDirectory.toFile());
        }
    }

    @Test
    public void testFiringFailureHandlerWhenGettingChunks()
            throws URISyntaxException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException
    {
        final String tempPathPrefix = null;
        final Path tempDirectory = Files.createTempDirectory(Paths.get("."), tempPathPrefix);

        try {
            final AtomicInteger numFailuresRecorded = new AtomicInteger();

            final FailureEventListener failureEventListener = new FailureEventListener() {
                @Override
                public void onFailure(final FailureEvent failureEvent) {
                    numFailuresRecorded.incrementAndGet();
                    assertEquals(FailureEvent.FailureActivity.GettingObject, failureEvent.doingWhat());
                }
            };

            final Ds3ClientHelpers.Job readJob = createReadJobWithObjectsReadyToTransfer(Ds3ClientShimFactory.ClientFailureType.ChunkAllocation);

            readJob.attachFailureEventListener(failureEventListener);

            try {
                readJob.transfer(new FileObjectGetter(tempDirectory));
            } catch (final IOException e) {
                assertEquals(1, numFailuresRecorded.get());
            }
        } finally {
            FileUtils.deleteDirectory(tempDirectory.toFile());
        }
    }

    private Ds3ClientHelpers.Job createReadJobWithObjectsReadyToTransfer(final Ds3ClientShimFactory.ClientFailureType clientFailureType)
            throws IOException, URISyntaxException, NoSuchMethodException, IllegalAccessException, InvocationTargetException
    {
        final String DIR_NAME = "largeFiles/";
        final String FILE_NAME = "lesmis-copies.txt";

        final Path objPath = ResourceUtils.loadFileResource(DIR_NAME + FILE_NAME);
        final long bookSize = Files.size(objPath);
        final Ds3Object obj = new Ds3Object(FILE_NAME, bookSize);

        final Ds3Client ds3Client = Ds3ClientShimFactory.makeWrappedDs3Client(clientFailureType, client);

        final int maxNumBlockAllocationRetries = 3;
        final int maxNumObjectTransferAttempts = 3;
        final Ds3ClientHelpers ds3ClientHelpers = Ds3ClientHelpers.wrap(ds3Client,
                maxNumBlockAllocationRetries,
                maxNumObjectTransferAttempts);

        final Ds3ClientHelpers.Job readJob = ds3ClientHelpers.startReadJob(BUCKET_NAME, Arrays.asList(obj));

        return readJob;
    }

    @Test
    public void testFiringFailureHandlerWhenGettingObject()
            throws URISyntaxException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException
    {
        final String tempPathPrefix = null;
        final Path tempDirectory = Files.createTempDirectory(Paths.get("."), tempPathPrefix);

        try {
            final AtomicInteger numFailuresRecorded = new AtomicInteger();

            final FailureEventListener failureEventListener = new FailureEventListener() {
                @Override
                public void onFailure(final FailureEvent failureEvent) {
                    numFailuresRecorded.incrementAndGet();
                    assertEquals(FailureEvent.FailureActivity.GettingObject, failureEvent.doingWhat());
                }
            };

            final Ds3ClientHelpers.Job readJob = createReadJobWithObjectsReadyToTransfer(Ds3ClientShimFactory.ClientFailureType.GetObject);

            readJob.attachFailureEventListener(failureEventListener);

            try {
                readJob.transfer(new FileObjectGetter(tempDirectory));
            } catch (final IOException e) {
                assertEquals(1, numFailuresRecorded.get());
            }
        } finally {
            FileUtils.deleteDirectory(tempDirectory.toFile());
        }
    }

    @Test
    public void testCreatingReadJobWithStreamedBehavior() throws IOException, URISyntaxException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        doReadJobWithJobStarter(new ReadJobStarter() {
            @Override
            public Ds3ClientHelpers.Job startReadJob(final Ds3ClientHelpers ds3ClientHelpers, final String bucketName, final Iterable<Ds3Object> objectsToread)
                throws IOException
            {
                return ds3ClientHelpers.startReadJobUsingStreamedBehavior(BUCKET_NAME, objectsToread);
            }
        });
    }

    private interface ReadJobStarter {
        Ds3ClientHelpers.Job startReadJob(final Ds3ClientHelpers ds3ClientHelpers, final String bucketName, Iterable<Ds3Object> objectsToread) throws IOException;
    }

    private void doReadJobWithJobStarter(final ReadJobStarter readJobStarter) throws IOException, URISyntaxException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        final String tempPathPrefix = null;
        final Path tempDirectory = Files.createTempDirectory(Paths.get("."), tempPathPrefix);

        try {
            final String DIR_NAME = "largeFiles/";
            final String FILE_NAME = "lesmis.txt";

            final Path objPath = ResourceUtils.loadFileResource(DIR_NAME + FILE_NAME);
            final long bookSize = Files.size(objPath);
            final Ds3Object obj = new Ds3Object(FILE_NAME, bookSize);

            final Ds3ClientShim ds3ClientShim = new Ds3ClientShim((Ds3ClientImpl)client);

            final int maxNumBlockAllocationRetries = 1;
            final int maxNumObjectTransferAttempts = 3;
            final Ds3ClientHelpers ds3ClientHelpers = Ds3ClientHelpers.wrap(ds3ClientShim,
                    maxNumBlockAllocationRetries,
                    maxNumObjectTransferAttempts);

            final Ds3ClientHelpers.Job readJob = readJobStarter.startReadJob(ds3ClientHelpers, BUCKET_NAME, Arrays.asList(obj));

            final AtomicBoolean dataTransferredEventReceived = new AtomicBoolean(false);
            final AtomicBoolean objectCompletedEventReceived = new AtomicBoolean(false);
            final AtomicBoolean checksumEventReceived = new AtomicBoolean(false);
            final AtomicBoolean metadataEventReceived = new AtomicBoolean(false);
            final AtomicBoolean waitingForChunksEventReceived = new AtomicBoolean(false);
            final AtomicBoolean failureEventReceived = new AtomicBoolean(false);

            readJob.attachDataTransferredListener(new DataTransferredListener() {
                @Override
                public void dataTransferred(final long size) {
                    dataTransferredEventReceived.set(true);
                    assertEquals(bookSize, size);
                }
            });
            readJob.attachObjectCompletedListener(new ObjectCompletedListener() {
                @Override
                public void objectCompleted(final String name) {
                    objectCompletedEventReceived.set(true);
                }
            });
            readJob.attachChecksumListener(new ChecksumListener() {
                @Override
                public void value(final BulkObject obj, final ChecksumType.Type type, final String checksum) {
                    checksumEventReceived.set(true);
                    assertEquals("69+JXWeZuzl2HFTM6Lbo8A==", checksum);
                }
            });
            readJob.attachMetadataReceivedListener(new MetadataReceivedListener() {
                @Override
                public void metadataReceived(final String filename, final Metadata metadata) {
                    metadataEventReceived.set(true);
                }
            });
            readJob.attachWaitingForChunksListener(new WaitingForChunksListener() {
                @Override
                public void waiting(final int secondsToWait) {
                    waitingForChunksEventReceived.set(true);
                }
            });
            readJob.attachFailureEventListener(new FailureEventListener() {
                @Override
                public void onFailure(final FailureEvent failureEvent) {
                    failureEventReceived.set(true);
                }
            });

            final GetJobSpectraS3Response jobSpectraS3Response = ds3ClientShim
                    .getJobSpectraS3(new GetJobSpectraS3Request(readJob.getJobId()));

            assertThat(jobSpectraS3Response.getStatusCode(), is(200));

            readJob.transfer(new FileObjectGetter(tempDirectory));

            final File originalFile = ResourceUtils.loadFileResource(DIR_NAME + FILE_NAME).toFile();
            final File fileCopiedFromBP = Paths.get(tempDirectory.toString(), FILE_NAME).toFile();
            assertTrue(FileUtils.contentEquals(originalFile, fileCopiedFromBP));

            assertTrue(dataTransferredEventReceived.get());
            assertTrue(objectCompletedEventReceived.get());
            assertTrue(checksumEventReceived.get());
            assertTrue(metadataEventReceived.get());
            assertFalse(waitingForChunksEventReceived.get());
            assertFalse(failureEventReceived.get());
        } finally {
            FileUtils.deleteDirectory(tempDirectory.toFile());
        }
    }

    @Test
    public void testCreatingReadJobWithRandomAccessBehavior() throws IOException, URISyntaxException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        doReadJobWithJobStarter(new ReadJobStarter() {
            @Override
            public Ds3ClientHelpers.Job startReadJob(final Ds3ClientHelpers ds3ClientHelpers, final String bucketName, final Iterable<Ds3Object> objectsToread)
                    throws IOException
            {
                return ds3ClientHelpers.startReadJobUsingRandomAccessBehavior(BUCKET_NAME, objectsToread);
            }
        });
    }
}
