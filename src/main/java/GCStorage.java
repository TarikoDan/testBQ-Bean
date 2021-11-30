import com.google.api.gax.paging.Page;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.Identity;
import com.google.cloud.Policy;
import com.google.cloud.storage.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/*
 * Created for educational purposes and is not used in current task.
 */
public class GCStorage {
    public static final Logger LOG = LoggerFactory.getLogger(PopularNames.class);
    private static Storage storage;

    static {
        try {
            Credentials credentials = GoogleCredentials
                    .fromStream(new FileInputStream(Util.KEY_FILE));
            storage = StorageOptions.newBuilder()
                    .setCredentials(credentials)
                    .setProjectId(Util.PROJECT_ID).build().getService();
            LOG.info("Connected to Storage of Project " + Util.PROJECT_ID);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        String bucketName = "bq-beam-bucket";
        String objectName= "upload/usNames10.avro";
        String srcFilePath = "src/main/resources/usNames10.avro";
        String destFilePath = "src/main/resources/dest/usNames10.avro";
        createBucket(bucketName);
        uploadObject(bucketName, objectName, srcFilePath);
        downloadFile(bucketName, objectName, destFilePath);

    }

    public static void createBucket(String bucketName) {
        Bucket bucket = storage.create(BucketInfo.of(bucketName));
//        Bucket bucket = storage.create(BucketInfo.newBuilder(bucketName).build());

        LOG.info("Created bucket " + bucket.getName());
    }

    public static void uploadObject(
            String bucketName, String objectName, String filePath) throws IOException {
        BlobId blobId = BlobId.of(bucketName, objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        storage.create(blobInfo, Files.readAllBytes(Paths.get(filePath)));

        LOG.info(
                "File " + filePath + " uploaded to bucket " + bucketName + " as " + objectName);
    }

    public static void downloadFile(String bucketName, String srcFilename, String destFilePath) {
        Blob blob = storage.get(BlobId.of(bucketName, srcFilename));
        blob.downloadTo(Paths.get(destFilePath));
        LOG.info("Downloaded object " + srcFilename
                + " from bucket name " + bucketName
                + " to " + destFilePath);
    }

    public static void downloadPublicObject(
            String bucketName, String publicObjectName, Path destFilePath) {
        Storage publicStorage = StorageOptions.getUnauthenticatedInstance().getService();

        Blob blob = publicStorage.get(BlobId.of(bucketName, publicObjectName));
        blob.downloadTo(destFilePath);

        LOG.info("Downloaded public object " + publicObjectName
                + " from bucket name " + bucketName
                + " to " + destFilePath);
    }

    public static void makeBucketPublic(String bucketName) {
        Policy originalPolicy = storage.getIamPolicy(bucketName);
        storage.setIamPolicy(
                bucketName,
                originalPolicy.toBuilder()
                        .addIdentity(StorageRoles.objectViewer(), Identity.allUsers()) // All users can view
                        .build());

        LOG.info("Bucket " + bucketName + " is now publicly readable");
    }

    public static void checkDefaultStorage() {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        System.out.println("Buckets in default Storage:");
        Page<Bucket> buckets = storage.list();
        for (Bucket bucket : buckets.iterateAll()) {
            System.out.println(bucket.toString());
        }
    }

    public static void checkStorage() {
        System.out.println("Buckets:");
        Page<Bucket> buckets = storage.list();
        for (Bucket bucket : buckets.iterateAll()) {
            System.out.println(bucket.toString());
        }
    }

}

