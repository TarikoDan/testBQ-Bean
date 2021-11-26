import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.Identity;
import com.google.cloud.Policy;
import com.google.cloud.storage.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class GCStorage {
    private static final String keyFile = "C:/Users/Taras_Danylyshyn/Documents/EquiFax/Task1_BQ-Bean/Keys/test-bq-331608-f6f571f4b2c0.json";
    private static final String projectId = "test-bq-331608";

    private static Storage storage;


    static {
        try {
            Credentials credentials = GoogleCredentials
                    .fromStream(new FileInputStream(keyFile));
            storage = StorageOptions.newBuilder()
                    .setCredentials(credentials)
                    .setProjectId(projectId).build().getService();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        String bucketName = "bq-beam-bucket";
        String bucketName2 = "bq-beam";
        String srcFilename= "usnames100.avro";
        String objectName= "upload/usnames10.avro";
        String destFilePath = "src/main/resources/names10.avro";
        String objFilePath = "C:/Users/Taras_Danylyshyn/Documents/EquiFax/Task1_BQ-Bean/usnames10.avro";
//        createBucketWithCred(projectId, bucketName);
//        uploadObject(projectId, bucketName, objectName, objFilePath);
        downloadFile(bucketName, objectName, destFilePath);

    }

    public static void createBucket(String bucketName) {
        Bucket bucket = storage.create(BucketInfo.of(bucketName));
//        Bucket bucket = storage.create(BucketInfo.newBuilder(bucketName).build());

        System.out.println("Created bucket " + bucket.getName());
    }

    public static void createBucketWithCred(String bucketName) {
        Bucket bucket = storage.create(BucketInfo.of(bucketName));
//        Bucket bucket = storage.create(BucketInfo.newBuilder(bucketName).build());

        System.out.println("Created bucket " + bucket.getName());
    }

    public static void downloadFile(String bucketName, String srcFilename, String destFilePath)
            throws IOException {
        Blob blob = storage.get(BlobId.of(bucketName, srcFilename));
        blob.downloadTo(Paths.get(destFilePath));
        System.out.println(
                "Downloaded object "
                        + srcFilename
                        + " from bucket name "
                        + bucketName
                        + " to "
                        + destFilePath);
    }

    public static void downloadPublicObject(
            String bucketName, String publicObjectName, Path destFilePath) {
        Storage publicStorage = StorageOptions.getUnauthenticatedInstance().getService();

        Blob blob = publicStorage.get(BlobId.of(bucketName, publicObjectName));
        blob.downloadTo(destFilePath);

        System.out.println(
                "Downloaded public object "
                        + publicObjectName
                        + " from bucket name "
                        + bucketName
                        + " to "
                        + destFilePath);
    }

    public static void makeBucketPublic(String bucketName) {
        Policy originalPolicy = storage.getIamPolicy(bucketName);
        storage.setIamPolicy(
                bucketName,
                originalPolicy.toBuilder()
                        .addIdentity(StorageRoles.objectViewer(), Identity.allUsers()) // All users can view
                        .build());

        System.out.println("Bucket " + bucketName + " is now publicly readable");
    }

    public static void uploadObject(
            String bucketName, String objectName, String filePath) throws IOException {
        BlobId blobId = BlobId.of(bucketName, objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        storage.create(blobInfo, Files.readAllBytes(Paths.get(filePath)));

        System.out.println(
                "File " + filePath + " uploaded to bucket " + bucketName + " as " + objectName);
    }
}

