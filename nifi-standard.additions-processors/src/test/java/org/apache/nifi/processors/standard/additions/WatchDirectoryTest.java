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

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardWatchEventKinds;
import java.util.List;
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
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
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
        	.thenRun(() -> addContent(filename, "Hello"))
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
}
