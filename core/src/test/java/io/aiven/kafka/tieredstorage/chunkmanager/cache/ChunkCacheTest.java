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

package io.aiven.kafka.tieredstorage.chunkmanager.cache;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import io.aiven.kafka.tieredstorage.Part;
import io.aiven.kafka.tieredstorage.chunkmanager.ChunkKey;
import io.aiven.kafka.tieredstorage.chunkmanager.ChunkManager;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifestV1;
import io.aiven.kafka.tieredstorage.manifest.index.FixedSizeChunkIndex;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ChunkCacheTest {

    private static final byte[] CHUNK_0 = "0123456789".getBytes();
    private static final byte[] CHUNK_1 = "1011121314".getBytes();
    private static final FixedSizeChunkIndex FIXED_SIZE_CHUNK_INDEX = new FixedSizeChunkIndex(10, 20, 12, 24);
    private static final SegmentManifest SEGMENT_MANIFEST = new SegmentManifestV1(FIXED_SIZE_CHUNK_INDEX, false, null);
    private static final String TEST_EXCEPTION_MESSAGE = "test_message";
    private static final String SEGMENT_KEY = "topic/segment";
    @Mock
    private ChunkManager chunkManager;
    private ChunkCache<?> chunkCache;

    @BeforeEach
    void setUp() {
        chunkCache = spy(new InMemoryChunkCache(chunkManager));
    }

    @AfterEach
    void tearDown() {
        reset(chunkManager);
    }

    @Nested
    class CacheTests {
        @Mock
        RemovalListener<ChunkKey, ?> removalListener;

        Part firstPart;
        Part nextPart;

        @BeforeEach
        void setUp() throws Exception {
            doAnswer(invocation -> removalListener).when(chunkCache).removalListener();
            final var chunkIndex = SEGMENT_MANIFEST.chunkIndex();
            firstPart = new Part(chunkIndex, chunkIndex.chunks().get(0), 1);
            when(chunkManager.chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, firstPart))
                .thenAnswer(invocation -> new ByteArrayInputStream(CHUNK_0));
            nextPart = firstPart.next().get();
            when(chunkManager.chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, nextPart))
                .thenAnswer(invocation -> new ByteArrayInputStream(CHUNK_1));
        }

        @Test
        void noEviction() throws IOException, StorageBackendException {
            chunkCache.configure(Map.of(
                "retention.ms", "-1",
                "size", "-1"
            ));

            final InputStream chunk0 = chunkCache.chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, firstPart);
            assertThat(chunk0).hasBinaryContent(CHUNK_0);
            verify(chunkManager).chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, firstPart);
            final InputStream cachedChunk0 = chunkCache.chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, firstPart);
            assertThat(cachedChunk0).hasBinaryContent(CHUNK_0);
            verifyNoMoreInteractions(chunkManager);

            final InputStream chunk1 = chunkCache.chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, nextPart);
            assertThat(chunk1).hasBinaryContent(CHUNK_1);
            verify(chunkManager).chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, nextPart);
            final InputStream cachedChunk1 = chunkCache.chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, nextPart);
            assertThat(cachedChunk1).hasBinaryContent(CHUNK_1);
            verifyNoMoreInteractions(chunkManager);

            verifyNoInteractions(removalListener);
        }

        @Test
        void timeBasedEviction() throws IOException, StorageBackendException, InterruptedException {
            chunkCache.configure(Map.of(
                "retention.ms", "100",
                "size", "-1"
            ));

            assertThat(chunkCache.chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, firstPart))
                .hasBinaryContent(CHUNK_0);
            verify(chunkManager).chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, firstPart);
            assertThat(chunkCache.chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, firstPart))
                .hasBinaryContent(CHUNK_0);
            verifyNoMoreInteractions(chunkManager);

            Thread.sleep(100);

            assertThat(chunkCache.chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, nextPart))
                .hasBinaryContent(CHUNK_1);
            verify(chunkManager).chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, nextPart);
            assertThat(chunkCache.chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, nextPart))
                .hasBinaryContent(CHUNK_1);
            verifyNoMoreInteractions(chunkManager);

            await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(100))
                .until(() -> !mockingDetails(removalListener).getInvocations().isEmpty());

            verify(removalListener)
                .onRemoval(
                    argThat(argument -> argument.chunkId == 0),
                    any(),
                    eq(RemovalCause.EXPIRED));

            assertThat(chunkCache.chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, firstPart))
                .hasBinaryContent(CHUNK_0);
            verify(chunkManager, times(2)).chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, firstPart);
        }

        @Test
        void sizeBasedEviction() throws IOException, StorageBackendException {
            chunkCache.configure(Map.of(
                "retention.ms", "-1",
                "size", "18"
            ));

            assertThat(chunkCache.chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, firstPart))
                .hasBinaryContent(CHUNK_0);
            verify(chunkManager).chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, firstPart);
            assertThat(chunkCache.chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, firstPart))
                .hasBinaryContent(CHUNK_0);
            verifyNoMoreInteractions(chunkManager);

            assertThat(chunkCache.chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, nextPart))
                .hasBinaryContent(CHUNK_1);
            verify(chunkManager).chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, nextPart);

            await().atMost(Duration.ofMillis(5000))
                .pollDelay(Duration.ofSeconds(2))
                .pollInterval(Duration.ofMillis(10))
                .until(() -> !mockingDetails(removalListener).getInvocations().isEmpty());

            verify(removalListener).onRemoval(any(ChunkKey.class), any(), eq(RemovalCause.SIZE));

            assertThat(chunkCache.chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, firstPart))
                .hasBinaryContent(CHUNK_0);
            assertThat(chunkCache.chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, nextPart))
                .hasBinaryContent(CHUNK_1);
            verify(chunkManager, times(3)).chunksContent(eq(SEGMENT_KEY), eq(SEGMENT_MANIFEST), any());
        }

    }

    @Nested
    class ErrorHandlingTests {
        Part firstPart;
        Part nextPart;

        private final Map<String, String> configs = Map.of(
            "retention.ms", "-1",
            "size", "-1"
        );

        @BeforeEach
        void setUp() {
            chunkCache.configure(configs);

            firstPart = new Part(SEGMENT_MANIFEST.chunkIndex(), SEGMENT_MANIFEST.chunkIndex().chunks().get(0), 1);
            nextPart = firstPart.next().get();
        }

        @Test
        void failedFetching() throws Exception {
            when(chunkManager.chunksContent(eq(SEGMENT_KEY), eq(SEGMENT_MANIFEST), any()))
                .thenThrow(new StorageBackendException(TEST_EXCEPTION_MESSAGE))
                .thenThrow(new IOException(TEST_EXCEPTION_MESSAGE));

            assertThatThrownBy(() -> chunkCache
                .chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, firstPart))
                .isInstanceOf(StorageBackendException.class)
                .hasMessage(TEST_EXCEPTION_MESSAGE);
            assertThatThrownBy(() -> chunkCache
                .chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, nextPart))
                .isInstanceOf(IOException.class)
                .hasMessage(TEST_EXCEPTION_MESSAGE);
        }

        @Test
        void failedReadingCachedValueWithInterruptedException() throws Exception {
            when(chunkManager.chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, firstPart))
                .thenReturn(new ByteArrayInputStream(CHUNK_0));

            doCallRealMethod().doAnswer(invocation -> {
                throw new InterruptedException(TEST_EXCEPTION_MESSAGE);
            }).when(chunkCache).cachedChunkToInputStream(any());

            chunkCache.chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, firstPart);
            assertThatThrownBy(() -> chunkCache
                .chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, firstPart))
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(ExecutionException.class)
                .hasRootCauseInstanceOf(InterruptedException.class)
                .hasRootCauseMessage(TEST_EXCEPTION_MESSAGE);
        }

        @Test
        void failedReadingCachedValueWithExecutionException() throws Exception {
            when(chunkManager.chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, firstPart)).thenReturn(
                new ByteArrayInputStream(CHUNK_0));
            doCallRealMethod().doAnswer(invocation -> {
                throw new ExecutionException(new RuntimeException(TEST_EXCEPTION_MESSAGE));
            }).when(chunkCache).cachedChunkToInputStream(any());

            chunkCache.chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, firstPart);
            assertThatThrownBy(() -> chunkCache
                .chunksContent(SEGMENT_KEY, SEGMENT_MANIFEST, firstPart))
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(ExecutionException.class)
                .hasRootCauseInstanceOf(RuntimeException.class)
                .hasRootCauseMessage(TEST_EXCEPTION_MESSAGE);
        }
    }
}
