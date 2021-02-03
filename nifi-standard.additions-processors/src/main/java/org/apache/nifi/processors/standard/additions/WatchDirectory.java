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

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"file", "get", "list", "ingest", "source", "filesystem", "inotify", "watchservice"})

// These lines is taken from org.apache.nifi.processors.standard.ListFile
@CapabilityDescription("Listens for files on the local filesystem using java WatchService. For each file that is listed, " +
        "creates a FlowFile that represents the file so that it can be fetched in conjunction with FetchFile. Unlike " +
        "GetFile, this Processor does not delete any data from the local filesystem.")
@SeeAlso({})
@WritesAttributes({
    @WritesAttribute(attribute="filename", description="The name of the file that was read from filesystem."),
    @WritesAttribute(attribute="absolute.path", description="The absolute.path is set to the absolute path of " +
            "the file's directory on filesystem. For example, if the Input Directory property is set to /tmp, " +
            "then files picked up from /tmp will have the path attribute set to \"/tmp/\". If the Recurse " +
            "Subdirectories property is set to true and a file is picked up from /tmp/abc/1/2/3, then the path " +
            "attribute will be set to \"/tmp/abc/1/2/3/\"."),
    @WritesAttribute(attribute=WatchDirectory.WATCH_PROPERTY_NAME, description="The name of the dynamic property that has been triggered"),
    @WritesAttribute(attribute=WatchDirectory.WATCH_EVENT_TYPE, description="The event type that triggered."),
	// These lines is taken from org.apache.nifi.processors.standard.ListFile
    @WritesAttribute(attribute=WatchDirectory.FILE_OWNER_ATTRIBUTE, description="The user that owns the file in filesystem"),
    @WritesAttribute(attribute=WatchDirectory.FILE_GROUP_ATTRIBUTE, description="The group that owns the file in filesystem"),
    @WritesAttribute(attribute=WatchDirectory.FILE_SIZE_ATTRIBUTE, description="The number of bytes in the file in filesystem"),
    @WritesAttribute(attribute=WatchDirectory.FILE_PERMISSIONS_ATTRIBUTE, description="The permissions for the file in filesystem. This " +
            "is formatted as 3 characters for the owner, 3 for the group, and 3 for other users. For example " +
            "rw-rw-r--"),
    @WritesAttribute(attribute=WatchDirectory.FILE_LAST_MODIFY_TIME_ATTRIBUTE, description="The timestamp of when the file in filesystem was " +
            "last modified as 'yyyy-MM-dd'T'HH:mm:ssZ'"),
//    @WritesAttribute(attribute=WatchDirectory.FILE_LAST_ACCESS_TIME_ATTRIBUTE, description="The timestamp of when the file in filesystem was " +
//            "last accessed as 'yyyy-MM-dd'T'HH:mm:ssZ'"),
//    @WritesAttribute(attribute=WatchDirectory.FILE_CREATION_TIME_ATTRIBUTE, description="The timestamp of when the file in filesystem was " +
//            "created as 'yyyy-MM-dd'T'HH:mm:ssZ'")
})
@DynamicProperty(
		name = "A name for the path, this is passed on as an attribute when a flowfile is created",
		value = "The absolute path to the directory that should be watched.", 
		description = "")
public class WatchDirectory extends AbstractSessionFactoryProcessor {
    public static final String WATCH_PROPERTY_NAME = "watch.property.name";
    public static final String WATCH_EVENT_TYPE = "watch.event.type";
    public static final String FILE_CREATION_TIME_ATTRIBUTE = "file.creationTime";
    public static final String FILE_LAST_MODIFY_TIME_ATTRIBUTE = "file.lastModifiedTime";
    public static final String FILE_LAST_ACCESS_TIME_ATTRIBUTE = "file.lastAccessTime";
    public static final String FILE_SIZE_ATTRIBUTE = "file.size";
    public static final String FILE_OWNER_ATTRIBUTE = "file.owner";
    public static final String FILE_GROUP_ATTRIBUTE = "file.group";
    public static final String FILE_PERMISSIONS_ATTRIBUTE = "file.permissions";
    public static final String FILE_MODIFY_DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";

    public static final PropertyDescriptor NOTIFY_ON_CREATE = new PropertyDescriptor
            .Builder().name("NOTIFY_ON_CREATE")
            .displayName("Created entries")
            .description("Create a flowfile when a new entry is made in the watched directory. "
            		+ "It could be due to the creation of a new file or renaming of an existing file.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues(Boolean.toString(Boolean.TRUE), Boolean.toString(Boolean.FALSE))
            .defaultValue(Boolean.toString(Boolean.TRUE))
            .build();

    public static final PropertyDescriptor NOTIFY_ON_MODIFIED = new PropertyDescriptor
            .Builder().name("NOTIFY_ON_MODIFIED")
            .displayName("Modified entries")
            .description("Create a flowfile when an existing entry in the watched directory is modified. "
            		+ "All file edit's trigger this event. "
            		+ "On some platforms, even changing file attributes will trigger it.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues(Boolean.toString(Boolean.TRUE), Boolean.toString(Boolean.FALSE))
            .defaultValue(Boolean.toString(Boolean.FALSE))
            .build();

    public static final PropertyDescriptor NOTIFY_ON_DELETED = new PropertyDescriptor
            .Builder().name("NOTIFY_ON_DELETED")
            .displayName("Deleted entries")
            .description("Create a flowfile when an entry is deleted, moved or renamed in the watched directory.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues(Boolean.toString(Boolean.TRUE), Boolean.toString(Boolean.FALSE))
            .defaultValue(Boolean.toString(Boolean.FALSE))
            .build();

    public static final PropertyDescriptor IGNORE_HIDDEN_FILES = new PropertyDescriptor
            .Builder().name("IGNORE_HIDDEN_FILES")
            .displayName("Ignore hidden files")
            .description("Don't create a flowfile if the filename starts with a dot ('.'). This property will be tested before the file filter.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues(Boolean.toString(Boolean.TRUE), Boolean.toString(Boolean.FALSE))
            .defaultValue(Boolean.toString(Boolean.TRUE))
            .build();

    public static final PropertyDescriptor FILE_FILTER = new Builder()
            .name("File Filter")
            .description("Only files whose names match the given regular expression will be picked up")
            .required(true)
            .defaultValue(".*")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final PropertyDescriptor MERGE_MODIFICATION_EVENTS = new Builder()
            .name("MergeModificationEvents")
            .description("If listening for modifications, this property will determine how long time (in millis) two or more modification "
            		+ "events will be considered as the same event. However, the merged event will not be emitted "
            		+ "until the last event has been received plus the number of millis give in this property.")
            .required(false)
            .defaultValue("500")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("")
            .build();


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private AtomicReference<ProcessSessionFactory> sessionFactoryReference = new AtomicReference<>();
	private ThreadGroup threadGroup;
	private DirectoryWatcherThread watcherThread;
	private volatile Map<String, Path> paths;
	private volatile boolean ignoreHiddenFiles = true;
	
	private DateTimeFormatter timestampFormat;
	private Pattern fileFilter;
	private boolean testing;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(NOTIFY_ON_CREATE);
        descriptors.add(NOTIFY_ON_MODIFIED);
        descriptors.add(NOTIFY_ON_DELETED);
        descriptors.add(IGNORE_HIDDEN_FILES);
        descriptors.add(FILE_FILTER);
        descriptors.add(MERGE_MODIFICATION_EVENTS);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }
    
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
                .required(false).dynamic(true).build();
    }


    @OnScheduled
    public void startWatcherService(ProcessContext context) {
    	this.timestampFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ").withZone(ZoneId.systemDefault());
    	
    	boolean notifyCreate = Boolean.toString(Boolean.TRUE).equalsIgnoreCase(context.getProperty(NOTIFY_ON_CREATE).getValue());
    	boolean notifyModified = Boolean.toString(Boolean.TRUE).equalsIgnoreCase(context.getProperty(NOTIFY_ON_MODIFIED).getValue());
    	boolean notifyDeleted = Boolean.toString(Boolean.TRUE).equalsIgnoreCase(context.getProperty(NOTIFY_ON_DELETED).getValue());
    	
    	ignoreHiddenFiles = Boolean.toString(Boolean.TRUE).equalsIgnoreCase(context.getProperty(IGNORE_HIDDEN_FILES).getValue());
    	int maxEventAge = Integer.decode(context.getProperty(MERGE_MODIFICATION_EVENTS).getValue());
    	
    	String regexFileFilter = context.getProperty(FILE_FILTER).getValue();
    	fileFilter = Pattern.compile(regexFileFilter);

    	Collection<WatchEvent.Kind<?>> kinds = new HashSet<WatchEvent.Kind<?>>(3);

    	if(notifyCreate)
    		kinds.add(StandardWatchEventKinds.ENTRY_CREATE);
    	if(notifyModified)
    		kinds.add(StandardWatchEventKinds.ENTRY_MODIFY);
    	if(notifyDeleted)
    		kinds.add(StandardWatchEventKinds.ENTRY_DELETE);

    	Map<PropertyDescriptor, String> properties = context.getProperties();
    	paths = new LinkedHashMap<>();
    	
    	for (PropertyDescriptor prop : properties.keySet()) {
    		if(!prop.isDynamic())
    			continue;
    		
    		String name = prop.getName();

    		String pathString = context.getProperty(prop).getValue();
    		Path path = Paths.get(pathString);
    		paths.put(name, path);
    	}

		threadGroup = new ThreadGroup(Thread.currentThread().getThreadGroup(), "NiFi Directory Watcher");
		watcherThread = new DirectoryWatcherThread(threadGroup, paths, kinds, maxEventAge, this::handleDirectoryEvents, getLogger());
		watcherThread.start();
    }
    
	@OnUnscheduled
    public void unschedule() {
		if(watcherThread != null) {
			watcherThread.requestStop();
			watcherThread.interrupt();
			watcherThread = null;
		}
    }

	@Override
	public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        sessionFactoryReference.compareAndSet(null, sessionFactory);
        context.yield();
        
        if(isTesting()) {
			try {
    			Thread.sleep(300);
    		} catch (InterruptedException e) {
    			e.printStackTrace();
    		}
		}
	}
	
	void setTesting(boolean testing) {
		this.testing = testing;
	}
	
	public boolean isTesting() {
		return testing;
	}

	private void handleDirectoryEvents(String propName, String eventKey, Collection<WatchEvent<?>> events) {
		ProcessSessionFactory sessionFactory = sessionFactoryReference.get();
		if(sessionFactory == null) {
			IllegalStateException exc = new IllegalStateException("No session factory has been set. Can not create any flow files");
			getLogger().error(exc.getMessage(), exc);
			return;
		}

		getLogger().debug("Creating process session");
		ProcessSession session = sessionFactory.createSession();
		Collection<FlowFile> createdFlowFiles = new LinkedList<>();
		
		Path affectedPath = paths.get(propName);
		
		for (WatchEvent<?> watchEvent : events) {
			String filename = watchEvent.context().toString();
			Kind<?> eventType = watchEvent.kind();
			
			if(ignoreHiddenFiles && filename.startsWith(".")) {
				continue;
			}
			
			if(!fileFilter.matcher(filename).matches()) {
				getLogger().debug("File " + filename + " is not matching the file filter, ignoring");
				continue;
			}
			
			FlowFile flowFile = session.create();
			
			Map<String, String> attributes = createAttributes(filename, affectedPath, eventType);
			attributes.put(WATCH_PROPERTY_NAME, propName);
			attributes.put(WATCH_EVENT_TYPE, eventType.name());
			
			flowFile = session.putAllAttributes(flowFile, attributes);
			createdFlowFiles.add(flowFile);
		}
		
		if(createdFlowFiles.size() > 0) {
			session.transfer(createdFlowFiles, SUCCESS);
			session.commit();
		}
	}

	private Map<String, String> createAttributes(String filename, Path affectedPath, Kind<?> eventType) {
		Path fullPath = affectedPath.resolve(filename);
		
		Map<String, String> attributes = new HashMap<>(10);
		
		attributes.put(CoreAttributes.FILENAME.key(), filename);
		attributes.put(CoreAttributes.ABSOLUTE_PATH.key(), affectedPath.toString());

		if(!StandardWatchEventKinds.ENTRY_DELETE.equals(eventType)){
			attributes.put(FILE_SIZE_ATTRIBUTE, Long.toString(getSize(fullPath)));
			
			String modifiedString = getModificationTime(fullPath);
			if(modifiedString != null)
				attributes.put(FILE_LAST_MODIFY_TIME_ATTRIBUTE, modifiedString);
			
			String owner = getOwner(fullPath);
			if(owner != null)
				attributes.put(FILE_OWNER_ATTRIBUTE, owner);
	
			getPosixAttributes(fullPath, attributes);
		}
		
		return attributes;
	}

	private String getModificationTime(Path fullPath) {
		try {
			FileTime modifiedTime = Files.getLastModifiedTime(fullPath);
			return timestampFormat.format(modifiedTime.toInstant());
		} catch (Throwable e) {
			onError(e);
			return null;
		}
	}

	private long getSize(Path fullPath) {
		try {
			return Files.size(fullPath);
		} catch (Throwable e) {
			onError(e);
		}
		
		return -1l;
	}

	private void getPosixAttributes(Path fullPath, Map<String, String> attributes) {
		try {
			FileStore store = Files.getFileStore(fullPath);
			// These lines is taken from org.apache.nifi.processors.standard.ListFile
			if (store.supportsFileAttributeView("posix")) {
                PosixFileAttributeView view = Files.getFileAttributeView(fullPath, PosixFileAttributeView.class);
                attributes.put(FILE_PERMISSIONS_ATTRIBUTE, PosixFilePermissions.toString(view.readAttributes().permissions()));
                attributes.put(FILE_GROUP_ATTRIBUTE, view.readAttributes().group().getName());
			}
		} catch (IOException e) {
			onError(e);
		}
	}

	private String getOwner(Path fullPath) {
		try {
			return Files.getFileAttributeView(fullPath, FileOwnerAttributeView.class).getOwner().getName();
		} catch (IOException e) {
			onError(e);
			return null;
		}
	}
	
    private void onError(Throwable e) {
		getLogger().error(e.getMessage(), e);
	}
}
