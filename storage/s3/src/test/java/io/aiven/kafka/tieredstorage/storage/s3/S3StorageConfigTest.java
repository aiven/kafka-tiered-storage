/*
 * Copyright 2023 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.tieredstorage.storage.s3;

import java.net.URI;
import java.time.Duration;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.StorageClass;

import static io.aiven.kafka.tieredstorage.storage.s3.S3StorageConfig.S3_MULTIPART_UPLOAD_PART_SIZE_DEFAULT;
import static io.aiven.kafka.tieredstorage.storage.s3.S3StorageConfig.S3_STORAGE_CLASS_DEFAULT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3StorageConfigTest {
    private static final String BUCKET_NAME = "b1";
    private static final Region TEST_REGION = Region.US_EAST_2;
    private static final String MINIO_URL = "http://minio";

    // Test scenarios
    // - Minimal config
    @Test
    void minimalConfig() {
        final var configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", TEST_REGION.id()
        );
        final var config = new S3StorageConfig(configs);

        assertThat(config.bucketName()).isEqualTo(BUCKET_NAME);
        assertThat(config.credentialsProvider()).isNull();
        assertThat(config.pathStyleAccessEnabled()).isNull();
        assertThat(config.uploadPartSize()).isEqualTo(S3_MULTIPART_UPLOAD_PART_SIZE_DEFAULT);
        assertThat(config.storageClass()).isEqualTo(S3_STORAGE_CLASS_DEFAULT);
        assertThat(config.certificateCheckEnabled()).isTrue();
        assertThat(config.checksumCheckEnabled()).isFalse();
        assertThat(config.region()).isEqualTo(TEST_REGION);
        assertThat(config.s3ServiceEndpoint()).isNull();
        assertThat(config.apiCallTimeout()).isNull();
        assertThat(config.apiCallAttemptTimeout()).isNull();
    }

    // - Credential provider scenarios
    //   - Without provider
    @Test
    void configWithoutCredentialsProvider() {
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", TEST_REGION.id(),
            "s3.endpoint.url", MINIO_URL,
            "s3.path.style.access.enabled", true
        );
        final var config = new S3StorageConfig(configs);
        assertThat(config.bucketName()).isEqualTo(BUCKET_NAME);
        assertThat(config.credentialsProvider()).isNull();
        assertThat(config.getBoolean(S3StorageConfig.S3_PATH_STYLE_ENABLED_CONFIG)).isTrue();
        assertThat(config.pathStyleAccessEnabled()).isTrue();
        assertThat(config.region()).isEqualTo(TEST_REGION);
        assertThat(config.s3ServiceEndpoint()).extracting(URI::getHost).isEqualTo("minio");
        assertThat(config.apiCallTimeout()).isNull();
        assertThat(config.apiCallAttemptTimeout()).isNull();
    }

    //   - With provider
    @Test
    void configWithProvider() {
        final var customCredentialsProvider = EnvironmentVariableCredentialsProvider.class;
        final int partSize = 10 * 1024 * 1024;
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", TEST_REGION.id(),
            "s3.endpoint.url", MINIO_URL,
            "s3.path.style.access.enabled", false,
            "s3.multipart.upload.part.size", partSize,
            "aws.credentials.provider.class", customCredentialsProvider.getName());

        final var config = new S3StorageConfig(configs);

        assertThat(config.bucketName()).isEqualTo(BUCKET_NAME);
        assertThat(config.pathStyleAccessEnabled()).isFalse();
        assertThat(config.uploadPartSize()).isEqualTo(partSize);
        assertThat(config.credentialsProvider()).isInstanceOf(customCredentialsProvider);
        assertThat(config.region()).isEqualTo(TEST_REGION);
        assertThat(config.s3ServiceEndpoint()).extracting(URI::getHost).isEqualTo("minio");
        assertThat(config.apiCallTimeout()).isNull();
        assertThat(config.apiCallAttemptTimeout()).isNull();
    }

    //   - With static credentials
    @Test
    void configWithStaticCredentials() {
        final Region region = Region.US_EAST_2;
        final String username = "username";
        final String password = "password";
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", region.id(),
            "s3.endpoint.url", MINIO_URL,
            "aws.access.key.id", username,
            "aws.secret.access.key", password,
            "aws.certificate.check.enabled", "false",
            "aws.checksum.check.enabled", "true");

        final var config = new S3StorageConfig(configs);

        assertThat(config.bucketName()).isEqualTo(BUCKET_NAME);
        assertThat(config.getString("s3.region")).isEqualTo(region.id());
        assertThat(config.getString("s3.endpoint.url")).isEqualTo(MINIO_URL);
        assertThat(config.getPassword("aws.access.key.id").value()).isEqualTo(username);
        assertThat(config.getPassword("aws.secret.access.key").value()).isEqualTo(password);
        assertThat(config.certificateCheckEnabled()).isFalse();
        assertThat(config.checksumCheckEnabled()).isTrue();

        final AwsCredentialsProvider credentialsProvider = config.credentialsProvider();
        assertThat(credentialsProvider).isInstanceOf(StaticCredentialsProvider.class);
        final var awsCredentials = credentialsProvider.resolveCredentials();
        assertThat(awsCredentials.accessKeyId()).isEqualTo(username);
        assertThat(awsCredentials.secretAccessKey()).isEqualTo(password);
        assertThat(config.region()).isEqualTo(TEST_REGION);
        assertThat(config.s3ServiceEndpoint()).extracting(URI::getHost).isEqualTo("minio");
        assertThat(config.apiCallTimeout()).isNull();
        assertThat(config.apiCallAttemptTimeout()).isNull();
    }

    //   - With missing static credentials
    @Test
    void configWithMissingStaticConfig() {
        final String username = "username";
        final String password = "password";
        assertThatThrownBy(() -> new S3StorageConfig(Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", TEST_REGION.id(),
            "s3.endpoint.url", MINIO_URL,
            "aws.access.key.id", username)))
            .isInstanceOf(ConfigException.class)
            .hasMessage("aws.access.key.id and aws.secret.access.key must be defined together");
        assertThatThrownBy(() -> new S3StorageConfig(Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", TEST_REGION.id(),
            "s3.endpoint.url", MINIO_URL,
            "aws.secret.access.key", password)))
            .isInstanceOf(ConfigException.class)
            .hasMessage("aws.access.key.id and aws.secret.access.key must be defined together");
    }

    //   - With empty static credentials
    @Test
    void configWithEmptyStaticConfig() {
        assertThatThrownBy(() -> new S3StorageConfig(Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", TEST_REGION.id(),
            "aws.access.key.id", "")))
            .isInstanceOf(ConfigException.class)
            .hasMessage("aws.access.key.id value must not be empty");
        assertThatThrownBy(() -> new S3StorageConfig(Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", TEST_REGION.id(),
            "aws.secret.access.key", "")))
            .isInstanceOf(ConfigException.class)
            .hasMessage("aws.secret.access.key value must not be empty");
    }

    //   - With conflict between static and custom
    @Test
    void configWithConflictBetweenCustomProviderAndStaticCredentials() {
        final String username = "username";
        final String password = "password";
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", TEST_REGION.id(),
            "s3.endpoint.url", MINIO_URL,
            "aws.credentials.provider.class", EnvironmentVariableCredentialsProvider.class.getName(),
            "aws.access.key.id", username,
            "aws.secret.access.key", password);
        assertThatThrownBy(() -> new S3StorageConfig(configs))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Either  static credential pair aws.access.key.id and aws.secret.access.key "
                + "must be set together, "
                + "or a custom provider class aws.credentials.provider.class. "
                + "If both are null, default S3 credentials provider is used.");
    }

    // - Failing configs scenarios
    @Test
    void shouldRequireS3BucketName() {
        assertThatThrownBy(() -> new S3StorageConfig(Map.of()))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Missing required configuration \"s3.bucket.name\" which has no default value.");
    }

    @Test
    void shouldRequirePartSizeLargerThan5MiB() {
        assertThatThrownBy(() -> new S3StorageConfig(Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", TEST_REGION.id(),
            "s3.multipart.upload.part.size", 1024
        )))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 1024 for configuration s3.multipart.upload.part.size: "
                + "Value must be at least 5242880");
    }

    @Test
    void withApiCallTimeouts() {
        final var configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", TEST_REGION.id(),
            "s3.api.call.timeout", 5000,
            "s3.api.call.attempt.timeout", 1000
        );
        final var config = new S3StorageConfig(configs);
        assertThat(config.apiCallTimeout()).isEqualTo(Duration.ofMillis(5000));
        assertThat(config.apiCallAttemptTimeout()).isEqualTo(Duration.ofMillis(1000));
    }

    @Test
    void withStorageClass() {
        final var configs = Map.of(
                "s3.bucket.name", BUCKET_NAME,
                "s3.region", TEST_REGION.id(),
                "s3.storage.class", StorageClass.STANDARD_IA.toString()
        );
        final var config = new S3StorageConfig(configs);
        assertThat(config.storageClass()).isEqualTo(StorageClass.STANDARD_IA.toString());
    }

    @Test
    void withRequireStorageClassInAllowList() {
        assertThatThrownBy(() -> new S3StorageConfig(Map.of(
                "s3.bucket.name", BUCKET_NAME,
                "s3.region", TEST_REGION.id(),
                "s3.storage.class", "WrongStorageClass"
        )))
                .isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value WrongStorageClass for configuration s3.storage.class: " +
                        "String must be one of: STANDARD, REDUCED_REDUNDANCY, STANDARD_IA, ONEZONE_IA, " +
                        "INTELLIGENT_TIERING, GLACIER, DEEP_ARCHIVE, OUTPOSTS, GLACIER_IR, SNOW, EXPRESS_ONEZONE");
    }
}
