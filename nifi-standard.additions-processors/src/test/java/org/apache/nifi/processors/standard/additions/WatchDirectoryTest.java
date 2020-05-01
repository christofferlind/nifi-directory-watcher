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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.lang3.SystemUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
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

	private ProcessContext context;

	private Path testDir = Paths.get("/tmp/testing-nifi");
	
	@BeforeClass
    public static void setupClass() {
        Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS);
    }

    @Before
    public void init() throws Exception {
        processor = new WatchDirectory();
        processor.setTesting(true);
        runner = TestRunners.newTestRunner(processor);
        context = runner.getProcessContext();
        deleteDirectory();
    }

    private void deleteDirectory() throws IOException {
    	if(Files.exists(testDir)) {
    		Files.list(testDir).forEach(t -> {
				try {
					Files.deleteIfExists(t);
				} catch (IOException e) {
					e.printStackTrace();
				}
			});
    		
    		Files.deleteIfExists(testDir);
    	}
    	
    	try {
			Files.createDirectory(testDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

    @Test
    public void testCreate() throws Exception {
        String propName = "test1";
		runner.setProperty(new PropertyDescriptor.Builder()
                .name(propName)
                .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
                .required(false).dynamic(true).build(), testDir.toString());
        
        String filename = "hello.txt";
        Thread t = new Thread(() ->  {
        	try {
        		Thread.sleep(WAIT);
				Path p = testDir.resolve(filename);
				Files.createFile(p);
        		Thread.sleep(WAIT);
				Files.deleteIfExists(p);
			} catch (Throwable e) {
				e.printStackTrace();
			}
        });
        t.setDaemon(true);
        
        t.start();
        runNext();
        runner.assertTransferCount(WatchDirectory.SUCCESS, 1);
        
        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(WatchDirectory.SUCCESS);
        MockFlowFile flowFile = successFiles1.get(0);
        assertEquals(filename, flowFile.getAttribute(CoreAttributes.FILENAME.key()));
        assertEquals(testDir.toString(), flowFile.getAttribute(CoreAttributes.ABSOLUTE_PATH.key()));
        assertEquals(StandardWatchEventKinds.ENTRY_CREATE.name(), flowFile.getAttribute(WatchDirectory.WATCH_EVENT_TYPE));
        assertEquals(propName, flowFile.getAttribute(WatchDirectory.WATCH_PROPERTY_NAME));
    }

    @Test
    public void testIgnoreDot() throws Exception {
        String propName = "test1";
		runner.setProperty(new PropertyDescriptor.Builder()
                .name(propName)
                .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
                .required(false).dynamic(true).build(), testDir.toString());
        
        String filename = ".hello.txt";
        Thread t = new Thread(() ->  {
        	try {
        		Thread.sleep(WAIT);
				Path p = testDir.resolve(filename);
				Files.createFile(p);
			} catch (Throwable e) {
				e.printStackTrace();
			}
        });
        t.setDaemon(true);
        
        t.start();
        runNext();
        runner.assertTransferCount(WatchDirectory.SUCCESS, 0);
    }

    @Test
    public void testRegexp() throws Exception {
        String propName = "test1";
        runner.setProperty(WatchDirectory.FILE_FILTER, ".*world.*");
		runner.setProperty(new PropertyDescriptor.Builder()
                .name(propName)
                .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
                .required(false).dynamic(true).build(), testDir.toString());
        
        Thread t = new Thread(() ->  {
        	try {
        		Thread.sleep(WAIT);
				Stream.of("hello.txt", "test2", "hello_world", "thisissomefile")
				.map(testDir::resolve)
				.forEach(p -> {
					try {
						Files.createFile(p);
					} catch (IOException e) {
						e.printStackTrace();
					}
				});
			} catch (Throwable e) {
				e.printStackTrace();
			}
        });
        t.setDaemon(true);
        
        t.start();
        runNext();
        runner.assertTransferCount(WatchDirectory.SUCCESS, 1);

        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(WatchDirectory.SUCCESS);
        MockFlowFile flowFile = successFiles1.get(0);
        assertEquals("hello_world", flowFile.getAttribute(CoreAttributes.FILENAME.key()));
    }

    @Test
    public void testNoneCreate() throws Exception {
        String propName = "test1";
        runner.setProperty(WatchDirectory.NOTIFY_ON_CREATE, Boolean.FALSE.toString());
        runner.setProperty(WatchDirectory.NOTIFY_ON_DELETED, Boolean.TRUE.toString());
		runner.setProperty(new PropertyDescriptor.Builder()
                .name(propName)
                .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
                .required(false).dynamic(true).build(), testDir.toString());
        
        String filename = "hello.txt";
        Thread t = new Thread(() ->  {
        	try {
        		Thread.sleep(WAIT);
				Files.createFile(testDir.resolve(filename));
			} catch (Throwable e) {
				e.printStackTrace();
			}
        });
        t.setDaemon(true);
        
        t.start();
        runNext();
        runner.assertTransferCount(WatchDirectory.SUCCESS, 0);
    }

    @Test
    public void testDelete() throws Exception {
        String propName = "test1";
        runner.setProperty(WatchDirectory.NOTIFY_ON_CREATE, Boolean.FALSE.toString());
        runner.setProperty(WatchDirectory.NOTIFY_ON_DELETED, Boolean.TRUE.toString());
		runner.setProperty(new PropertyDescriptor.Builder()
                .name(propName)
                .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
                .required(false).dynamic(true).build(), testDir.toString());
        
        String filename = "hello_world.txt";
        Thread t = new Thread(() ->  {
        	try {
        		Thread.sleep(WAIT);
        		Path p = testDir.resolve(filename);
				Files.createFile(p);
				Files.delete(p);
			} catch (Throwable e) {
				e.printStackTrace();
			}
        });
        t.setDaemon(true);
        
        t.start();
        runNext();
        runner.assertTransferCount(WatchDirectory.SUCCESS, 1);

        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(WatchDirectory.SUCCESS);
        MockFlowFile flowFile = successFiles1.get(0);
        assertEquals(filename, flowFile.getAttribute(CoreAttributes.FILENAME.key()));
        assertEquals(testDir.toString(), flowFile.getAttribute(CoreAttributes.ABSOLUTE_PATH.key()));
        assertEquals(StandardWatchEventKinds.ENTRY_DELETE.name(), flowFile.getAttribute(WatchDirectory.WATCH_EVENT_TYPE));
        assertEquals(propName, flowFile.getAttribute(WatchDirectory.WATCH_PROPERTY_NAME));
        
        assertNull(flowFile.getAttribute(WatchDirectory.FILE_SIZE_ATTRIBUTE));
    }

    private void runNext() throws InterruptedException {
        runner.clearTransferState();
        runner.run();
    }

}
