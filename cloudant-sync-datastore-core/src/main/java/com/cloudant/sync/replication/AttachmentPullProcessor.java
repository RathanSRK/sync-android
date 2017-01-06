package com.cloudant.sync.replication;

import com.cloudant.mazha.CouchClient;
import com.cloudant.sync.datastore.Attachment;
import com.cloudant.sync.datastore.AttachmentException;
import com.cloudant.sync.datastore.PreparedAttachment;
import com.cloudant.sync.datastore.UnsavedStreamAttachment;

import java.io.InputStream;

public class AttachmentPullProcessor implements CouchClient
        .InputStreamProcessor<PreparedAttachment> {

    private final DatastoreWrapper datastoreWrapper;
    private final String name;
    private final String contentType;
    private final Attachment.Encoding encoding;
    private final long length;
    private final long encodedLength;

    AttachmentPullProcessor(DatastoreWrapper wrapper, String name, String contentType, String
            encoding, long length, long encodedLength) {
        this.datastoreWrapper = wrapper;
        this.name = name;
        this.contentType = contentType;
        this.encoding = Attachment.getEncodingFromString(encoding);
        this.length = length;
        this.encodedLength = encodedLength;
    }

    @Override
    public PreparedAttachment processStream(InputStream stream) throws AttachmentException {
        UnsavedStreamAttachment usa = new UnsavedStreamAttachment(stream, name, contentType,
                encoding);
        PreparedAttachment attachment = datastoreWrapper.prepareAttachment(usa, length,
                encodedLength);
        return attachment;
    }
}
