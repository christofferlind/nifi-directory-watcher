/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.standard.additions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardWatchEventKinds;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.commons.lang3.SystemUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;


public class WatchDirectoryTest {

    private static final int WAIT = 200;

	private TestRunner runner;

	private WatchDirectory processor;

//	private ProcessContext context;

	private String dynamicPropertyName = "test1";
	private static final Path testDir = Paths.get("/tmp/testing-nifi");
	
	@BeforeClass
    public static void setupClass() {
        Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS);
    }

    @Before
    public void init() throws Exception {
        processor = new WatchDirectory();
        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(WatchDirectory.MERGE_MODIFICATION_EVENTS, Integer.toString(WAIT/2));
        runner.setRunSchedule(WAIT);
        deleteTestDirectory();
        
		runner.setProperty(new PropertyDescriptor.Builder()
                .name(dynamicPropertyName)
                .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
                .required(false).dynamic(true).build(), testDir.toString());
    }
    
    @After
    public void cleanup() {
    	deleteTestDirectory();
    }
    
    @Test
    public void testCreate() throws Exception {
        String filename = "hello.txt";
        runner.setProperty(WatchDirectory.NOTIFY_ON_CREATE, Boolean.TRUE.toString());
        runner.setProperty(WatchDirectory.NOTIFY_ON_MODIFIED, Boolean.FALSE.toString());
        runner.setProperty(WatchDirectory.NOTIFY_ON_DELETED, Boolean.FALSE.toString());
        runner.setProperty(WatchDirectory.MAX_WAIT, Integer.toString(10_000));

        CompletableFuture<Throwable> future = CompletableFuture
        	.runAsync(WatchDirectoryTest::sleepThread)
        	.thenRun(() -> createFile(filename))
        	.thenRun(WatchDirectoryTest::sleepThread)
	    	.handle((value, exc) -> onError(exc));
        
        clearTransferStateAndRun();
        
        assertNull(future.get(10, TimeUnit.SECONDS));
        runner.assertTransferCount(WatchDirectory.FAILURE, 0);
        runner.assertTransferCount(WatchDirectory.SUCCESS, 1);
        
        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(WatchDirectory.SUCCESS);
        MockFlowFile flowFile = successFiles1.get(0);
        assertEquals(filename, flowFile.getAttribute(CoreAttributes.FILENAME.key()));
        assertEquals(testDir.toString(), flowFile.getAttribute(CoreAttributes.ABSOLUTE_PATH.key()));
        assertEquals(StandardWatchEventKinds.ENTRY_CREATE.name(), flowFile.getAttribute(WatchDirectory.WATCH_EVENT_TYPE));
        assertEquals(dynamicPropertyName, flowFile.getAttribute(WatchDirectory.WATCH_PROPERTY_NAME));
    }

    @Test
    @Ignore("Make sure to the enviroment variable wd.test.irl to a valid rsync command without the destination and wd.test.irl.count")
    public void testIRLRsync() throws Throwable {
    	String envVariable = "wd.test.irl";
    	String envCountVariable = "wd.test.irl.count";
    	
    	String userInput = System.getenv(envVariable);
    	
    	if(userInput == null || userInput.isBlank()) {
			throw new IllegalArgumentException("Environment variable \"" + envVariable + "\" must be set. (Example: \"rsync -rhitP /path/to/source/\")");
		}

    	int expectedFiles = Integer.decode(System.getenv(envCountVariable));
    	
    	if(expectedFiles < 0)
    		throw new IllegalArgumentException("Are you sure you want to run a test with a negative amount of files?!");
    	
    	runner.setProperty(WatchDirectory.NOTIFY_ON_CREATE, Boolean.TRUE.toString());
    	runner.setProperty(WatchDirectory.NOTIFY_ON_MODIFIED, Boolean.TRUE.toString());
    	runner.setProperty(WatchDirectory.NOTIFY_ON_DELETED, Boolean.FALSE.toString());

    	List<String> cmd = new ArrayList<>();
    	cmd.add("sh");
    	cmd.add("-c");
    	cmd.add(userInput + " " + testDir.toString() + "/");

    	CompletableFuture<Throwable> future = CompletableFuture
    	.runAsync(WatchDirectoryTest::sleepThread)
    	.thenRun(() -> shellExecute(cmd.toArray(new String[cmd.size()])))
    	.handle(WatchDirectoryTest::ifErrorReturnException);

    	//Trigger processor once every second
    	runner.setRunSchedule(1_000);
    	//Check 10 times for new files
    	clearTransferStateAndRun(20);

    	assertDoneWithNoException(future);
    	
    	runner.assertTransferCount(WatchDirectory.FAILURE, 0);
    	runner.assertTransferCount(WatchDirectory.SUCCESS, expectedFiles);
    }

    
    @Test
    public void testRsync() throws Throwable {
    	runner.setProperty(WatchDirectory.NOTIFY_ON_CREATE, Boolean.TRUE.toString());
    	runner.setProperty(WatchDirectory.NOTIFY_ON_MODIFIED, Boolean.TRUE.toString());
    	runner.setProperty(WatchDirectory.NOTIFY_ON_DELETED, Boolean.FALSE.toString());

    	runner.setRunSchedule(1_000);

    	String filename = "hello.txt";

    	String fileSrc = testDir.toString() + "/big-file.bin";
    	String fileDst = testDir.toString() + "/" + filename;

    	int fileSize = 300;
		shellExecute("fallocate -l " + fileSize + "M " + fileSrc, true);

    	CompletableFuture<Throwable> future = CompletableFuture
    	.runAsync(WatchDirectoryTest::sleepThread)
    	.thenRun(() -> shellExecute("rsync -hitP " + fileSrc + " " + fileDst, true))
    	.handle(WatchDirectoryTest::ifErrorReturnException);

    	clearTransferStateAndRun(10);
    	
    	assertDoneWithNoException(future);

    	runner.assertTransferCount(WatchDirectory.FAILURE, 0);
    	runner.assertTransferCount(WatchDirectory.SUCCESS, 1);

    	final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(WatchDirectory.SUCCESS);
    	MockFlowFile flowFile = successFiles1.get(0);
    	assertEquals(filename, flowFile.getAttribute(CoreAttributes.FILENAME.key()));
    	assertEquals(testDir.toString(), flowFile.getAttribute(CoreAttributes.ABSOLUTE_PATH.key()));
    	assertEquals(StandardWatchEventKinds.ENTRY_CREATE.name(), flowFile.getAttribute(WatchDirectory.WATCH_EVENT_TYPE));
    	assertEquals(dynamicPropertyName, flowFile.getAttribute(WatchDirectory.WATCH_PROPERTY_NAME));
    	assertEquals(fileSize * 1024 * 1024, Integer.decode(flowFile.getAttribute(WatchDirectory.FILE_SIZE_ATTRIBUTE)).intValue());
    }
    
    
    @Test
    @Ignore("Most of the times, this completes as expected. But some times the second move comes before the first one.")
    public void testMultiFiles() throws Throwable {
    	runner.setProperty(WatchDirectory.NOTIFY_ON_CREATE, Boolean.TRUE.toString());
    	runner.setProperty(WatchDirectory.NOTIFY_ON_MODIFIED, Boolean.TRUE.toString());
    	runner.setProperty(WatchDirectory.NOTIFY_ON_DELETED, Boolean.FALSE.toString());
    	runner.setProperty(WatchDirectory.FILE_FILTER, "dst-.*\\.bin");

    	runner.setRunSchedule(WAIT*2);

    	String fileNameFormatDst = "dst-file-%d.bin";
    	
    	Map<Integer, String> filesCreatedOrder = new LinkedHashMap<>(4);
    	filesCreatedOrder.put(500, "rsync -hiPt");
    	filesCreatedOrder.put(1, "mv");
    	filesCreatedOrder.put(200, "cp");
    	filesCreatedOrder.put(150, "mv");
    	
    	List<Integer> expectedFileSizeOrder = new ArrayList<>(filesCreatedOrder.size());
    	expectedFileSizeOrder.add(1);
    	expectedFileSizeOrder.add(150);
    	expectedFileSizeOrder.add(200);
    	expectedFileSizeOrder.add(500);

    	List<CompletableFuture<?>> futures = new ArrayList<>(filesCreatedOrder.size());
    	
    	CompletableFuture<Throwable> future = CompletableFuture
    			.runAsync(WatchDirectoryTest::sleepThread)
    			.thenRun(() -> {
    				for (Entry<Integer, String> ent : filesCreatedOrder.entrySet()) {
    					Integer fileSize = ent.getKey();
    					String command = ent.getValue();

    					String fileSrc = String.format("%s/src-file-%d.bin", testDir.toString(), fileSize);
    					String fileDst = String.format("%s/" + fileNameFormatDst, testDir.toString(), fileSize);

    					shellExecute("fallocate -l " + fileSize + "M " + fileSrc, true);

    					String finalCommand = String.format("%s %s %s", command, fileSrc, fileDst);

    					shellExecute(finalCommand, false);
    				}
    			})
    			.handle(WatchDirectoryTest::ifErrorReturnException);
    	
    	futures.add(future);

    	clearTransferStateAndRun(15);
    	
    	assertDoneWithNoException(future);
    	
    	runner.assertTransferCount(WatchDirectory.FAILURE, 0);
    	runner.assertTransferCount(WatchDirectory.SUCCESS, filesCreatedOrder.size());

    	final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(WatchDirectory.SUCCESS);
    	
    	for (int i = 0; i < successFiles.size(); i++) {
    		MockFlowFile flowFile = successFiles.get(i);
    		Integer expectedFileSize = expectedFileSizeOrder.get(i);
			
    		assertEquals(String.format(fileNameFormatDst, expectedFileSize), flowFile.getAttribute(CoreAttributes.FILENAME.key()));
    		assertEquals(testDir.toString(), flowFile.getAttribute(CoreAttributes.ABSOLUTE_PATH.key()));
    		assertEquals(dynamicPropertyName, flowFile.getAttribute(WatchDirectory.WATCH_PROPERTY_NAME));
    		assertEquals(expectedFileSize * 1024 * 1024, Integer.decode(flowFile.getAttribute(WatchDirectory.FILE_SIZE_ATTRIBUTE)).intValue());
		}
    }

    private void shellExecute(String cmd, boolean wait) {
    	try {
    		Process process = Runtime.getRuntime().exec(cmd);
    		if(wait) waitFor(process);
    	} catch (Exception e) {
    		throw new IllegalStateException(e);
    	}
    }
    
    private void shellExecute(String[] cmd) {
		try {
			Process process = Runtime.getRuntime().exec(cmd);
			waitFor(process);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private void waitFor(Process process) throws InterruptedException, IOException {
		if(process.waitFor() != 0) {
			String errorOuput = "";
			try(InputStream errorStream = process.getErrorStream();
					InputStream stream = process.getInputStream();
					ByteArrayOutputStream bos = new ByteArrayOutputStream()){
				errorStream.transferTo(bos);
				stream.transferTo(System.out);
				
				errorOuput = new String(bos.toByteArray(), StandardCharsets.UTF_8);
			}
			
			System.err.println(errorOuput);
			throw new IllegalStateException("Command did not complete normally: " + errorOuput);
		}
	}

	@Test
    public void testMultiModifications() throws Exception {
        runner.setProperty(WatchDirectory.NOTIFY_ON_CREATE, Boolean.TRUE.toString());
        runner.setProperty(WatchDirectory.NOTIFY_ON_MODIFIED, Boolean.TRUE.toString());
        runner.setProperty(WatchDirectory.NOTIFY_ON_DELETED, Boolean.FALSE.toString());
        runner.setProperty(WatchDirectory.MERGE_MODIFICATION_EVENTS, Integer.toString(WAIT*2));
        runner.setProperty(WatchDirectory.MAX_WAIT, Integer.toString(10_000));
        
        runner.setRunSchedule(WAIT*2);
        
        String filename = "hello.txt";
        CompletableFuture<Throwable> future = CompletableFuture
        	.runAsync(WatchDirectoryTest::sleepThread)
        	.thenRun(() -> createFile(filename))
        	.thenRun(() -> addContent(filename, "Hello1"))
        	.thenRun(WatchDirectoryTest::sleepThread)
        	.thenRun(() -> addContent(filename, "Hello2"))
        	.thenRun(WatchDirectoryTest::sleepThread)
        	.thenRun(() -> addContent(filename, "Hello3"))
        	.thenRun(WatchDirectoryTest::sleepThread)
        	.thenRun(() -> addContent(filename, "Hello4"))
	    	.handle((value, exc) -> onError(exc));
        
        clearTransferStateAndRun(5);
        
        assertNull(future.get(10, TimeUnit.SECONDS));
        runner.assertTransferCount(WatchDirectory.FAILURE, 0);
        runner.assertTransferCount(WatchDirectory.SUCCESS, 1);
        
        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(WatchDirectory.SUCCESS);
        MockFlowFile flowFile = successFiles1.get(0);
        assertEquals(filename, flowFile.getAttribute(CoreAttributes.FILENAME.key()));
        assertEquals(testDir.toString(), flowFile.getAttribute(CoreAttributes.ABSOLUTE_PATH.key()));
        assertEquals(StandardWatchEventKinds.ENTRY_CREATE.name() + "," + StandardWatchEventKinds.ENTRY_MODIFY.name(), flowFile.getAttribute(WatchDirectory.WATCH_EVENT_TYPE));
        assertEquals(dynamicPropertyName, flowFile.getAttribute(WatchDirectory.WATCH_PROPERTY_NAME));
    }

    @Test
    public void testDotRename() throws Exception {        
        String filenameFrom = ".hello.txt";
        String filenameTo = "world.txt";

        runner.setProperty(WatchDirectory.NOTIFY_ON_CREATE, Boolean.TRUE.toString());
        runner.setProperty(WatchDirectory.NOTIFY_ON_MODIFIED, Boolean.FALSE.toString());
        runner.setProperty(WatchDirectory.NOTIFY_ON_DELETED, Boolean.FALSE.toString());
        
        CompletableFuture<Throwable> future = CompletableFuture
    	.runAsync(WatchDirectoryTest::sleepThread)
    	.thenRun(() -> {
    		Path p = createFile(filenameFrom);	
			try {
				Files.move(p, testDir.resolve(filenameTo), StandardCopyOption.ATOMIC_MOVE);
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
    	})
    	.thenRun(WatchDirectoryTest::sleepThread)
    	.handle((value, exc) -> onError(exc));
    
	    clearTransferStateAndRun();
	    
	    assertNull(future.get(10, TimeUnit.SECONDS));
        runner.assertTransferCount(WatchDirectory.FAILURE, 0);
        runner.assertTransferCount(WatchDirectory.SUCCESS, 1);
        
        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(WatchDirectory.SUCCESS);
        MockFlowFile flowFile = successFiles1.get(0);
        assertEquals(filenameTo, flowFile.getAttribute(CoreAttributes.FILENAME.key()));
        assertEquals(testDir.toString(), flowFile.getAttribute(CoreAttributes.ABSOLUTE_PATH.key()));
        assertEquals(StandardWatchEventKinds.ENTRY_CREATE.name(), flowFile.getAttribute(WatchDirectory.WATCH_EVENT_TYPE));
        assertEquals(dynamicPropertyName, flowFile.getAttribute(WatchDirectory.WATCH_PROPERTY_NAME));
    }

    @Test
    public void testIgnoreDot() throws Exception {
        String filename = ".hello.txt";
        CompletableFuture<Throwable> future = CompletableFuture
	    	.runAsync(WatchDirectoryTest::sleepThread)
	    	.thenRun(() -> createFile(filename))
	    	.thenRun(WatchDirectoryTest::sleepThread)
	    	.thenRun(WatchDirectoryTest::deleteTestDirectory)
	    	.handle((value, exc) -> onError(exc));
        
        clearTransferStateAndRun();
        
        assertNull(future.get(10, TimeUnit.SECONDS));
        runner.assertTransferCount(WatchDirectory.SUCCESS, 0);
    }

    @Test
    public void testRegexp() throws Throwable {
        runner.setProperty(WatchDirectory.FILE_FILTER, ".*world.*");

        CompletableFuture
	    	.runAsync(WatchDirectoryTest::sleepThread)
	    	.thenRun(() -> {
	    		Stream.of(
	    				"hello.txt", 
	    				"test2", 
	    				"hello_world", 
	    				"thisissomefile")
					.forEach(WatchDirectoryTest::createFile);
	    	})
	    	.thenRun(WatchDirectoryTest::sleepThread)
	    	.thenRun(WatchDirectoryTest::deleteTestDirectory);

        clearTransferStateAndRun();

        runner.assertTransferCount(WatchDirectory.SUCCESS, 1);

        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(WatchDirectory.SUCCESS);
        MockFlowFile flowFile = successFiles1.get(0);
        assertEquals("hello_world", flowFile.getAttribute(CoreAttributes.FILENAME.key()));
    }

    @Test
    public void testNoneCreate() throws Exception {
        runner.setProperty(WatchDirectory.NOTIFY_ON_CREATE, Boolean.FALSE.toString());
        runner.setProperty(WatchDirectory.NOTIFY_ON_MODIFIED, Boolean.FALSE.toString());
        runner.setProperty(WatchDirectory.NOTIFY_ON_DELETED, Boolean.TRUE.toString());
        
        String filename = "hello.txt";
        CompletableFuture<Throwable> future = CompletableFuture
	    	.runAsync(WatchDirectoryTest::sleepThread)
	    	.thenRun(() -> createFile(filename))
	    	.thenRun(WatchDirectoryTest::sleepThread)
	    	.handle((value, exc) -> onError(exc));
        
        clearTransferStateAndRun();
        
        assertNull(future.get(10, TimeUnit.SECONDS));

        runner.assertTransferCount(WatchDirectory.SUCCESS, 0);
    }

    @Test
    public void testDelete() throws Exception {
        runner.setProperty(WatchDirectory.NOTIFY_ON_CREATE, Boolean.FALSE.toString());
        runner.setProperty(WatchDirectory.NOTIFY_ON_DELETED, Boolean.TRUE.toString());

        String filename = "hello_world.txt";

        CompletableFuture<Throwable> future = CompletableFuture
	    	.runAsync(WatchDirectoryTest::sleepThread)
	    	.thenRun(() -> {
	    		Path p = createFile(filename);
	    		try {
					Files.delete(p);
				} catch (IOException e) {
					throw new IllegalStateException(e);
				}
	    	})
	    	.thenRun(WatchDirectoryTest::sleepThread)
	    	.thenRun(WatchDirectoryTest::deleteTestDirectory)
	    	.handle((value, exc) -> onError(exc));
        
        clearTransferStateAndRun();
        
        assertNull(future.get(10, TimeUnit.SECONDS));
        
        runner.assertTransferCount(WatchDirectory.SUCCESS, 1);

        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(WatchDirectory.SUCCESS);
        MockFlowFile flowFile = successFiles1.get(0);
        assertEquals(filename, flowFile.getAttribute(CoreAttributes.FILENAME.key()));
        assertEquals(testDir.toString(), flowFile.getAttribute(CoreAttributes.ABSOLUTE_PATH.key()));
        assertEquals(StandardWatchEventKinds.ENTRY_DELETE.name(), flowFile.getAttribute(WatchDirectory.WATCH_EVENT_TYPE));
        assertEquals(dynamicPropertyName, flowFile.getAttribute(WatchDirectory.WATCH_PROPERTY_NAME));
        
        assertNull(flowFile.getAttribute(WatchDirectory.FILE_SIZE_ATTRIBUTE));
    }

    private void clearTransferStateAndRun() throws InterruptedException {
    	clearTransferStateAndRun(5);
    }
    
    private void clearTransferStateAndRun(int count) throws InterruptedException {
        runner.clearTransferState();
        runner.run(count);
    }
    
    
    private static final void deleteTestDirectory() {
    	if(Files.exists(testDir)) {
    		try {
				Files.list(testDir).forEach(t -> {
					try {
						Files.deleteIfExists(t);
					} catch (IOException e) {
						e.printStackTrace();
					}
				});
			} catch (IOException e) {
				e.printStackTrace();
			}
    		
    		try {
				Files.deleteIfExists(testDir);
			} catch (IOException e) {
				e.printStackTrace();
			}
    	}
    	
    	try {
			Files.createDirectory(testDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
    
    private static final void sleepThread() {
    	sleepThread(WAIT);
    }
    
    private static final void sleepThread(int sleep) {
		try {
			Thread.sleep(sleep);
		} catch (InterruptedException e) {
			throw new IllegalStateException(e);
		}
    }
    
    private static final Throwable onError(Throwable e) {
    	if(e != null)
    		e.printStackTrace();
    	
    	return e;
    }
    
    private static final Path addContent(String filename, String content) {
		try {
			Path p = testDir.resolve(filename);
			
			try(BufferedWriter newBufferedWriter = Files.newBufferedWriter(p, StandardOpenOption.APPEND)){
				newBufferedWriter.write("Hello");
			}
			
			return p;
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
    	
    }

    private static final Path createFile(String filename) {
		try {
			Path p = testDir.resolve(filename);
			Files.createFile(p);
			return p;
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
		
    }
    
    private static final void assertDoneWithNoException(CompletableFuture<Throwable> future) throws Throwable {
    	assertTrue(future.isDone());
    	
    	Throwable throwable = future.get();
    	if(throwable != null)
    		throw throwable;
    }
    
    private static final Throwable ifErrorReturnException(Object result, Throwable exc) {
    	return exc;
    }
}
