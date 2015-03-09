import com.google.api.client.util.Sets;
import com.google.api.services.genomics.Genomics;
import com.google.api.services.genomics.model.*;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Created by brockman on 3/1/15.
 */
public class SampleReadPipeline {
  public static void main(String[] args) throws IOException, GeneralSecurityException {
    GenomicsReadsetOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation().as(GenomicsReadsetOptions.class);
    GenomicsOptions.Methods.validateOptions(options);

    GenomicsFactory.OfflineAuth auth = GenomicsOptions.Methods.getGenomicsAuth(options);
    Genomics genomics = auth.getGenomics(auth.getDefaultFactory());

    String datasetId = options.getDatasetId();
    String readGroupSetId = options.getReadGroupSetId();
    ReadGroupSet readGroupSet = genomics.readgroupsets().get(readGroupSetId).execute();
    String referenceSetId = readGroupSet.getReferenceSetId();
    ReferenceSet referenceSet = genomics.referencesets().get(referenceSetId).execute();
    Iterable<Reference> references;
    Iterable<Range> inputRanges;
    if (options.isAllReferences()) {
      references = lookupReferences(genomics, referenceSet, Optional.<Iterable<Range>>absent());
      // TODO: go through references and shard to generate ranges
    } else {
      inputRanges = parseRangesFromCommandLine(options.getReferences());
      references = lookupReferences(genomics, referenceSet, Optional.of(inputRanges));
    }
    Iterable<Range> ranges = getShards(inputRanges);

    List<SearchReadsRequest> readRequests =
        getSearchReadsRequests(readGroupSetId, ranges);

  }

  private static Iterable<Range> parseRangesFromCommandLine(String rangeOptions) {
    return FluentIterable.from(Splitter.on(",").split(rangeOptions)).transform(
        new Function<String, Range>() {
          @Override
          public Range apply(String rangeString) {
            ArrayList<String> rangeInfo = newArrayList(Splitter.on(":").split(rangeString));
            Range range = new Range();
            range.setReferenceName(rangeInfo.get(0));
            range.setStart(Long.valueOf(rangeInfo.get(1)));
            range.setEnd(Long.valueOf(rangeInfo.get(2)));
            return range;
          }
        });
  }

  private static Iterable<Reference> lookupReferences(
      Genomics genomics, ReferenceSet referenceSet, Optional<Iterable<Range>> referenceRanges) throws IOException {
    Optional<? extends Set<String>> referenceNames = referenceRanges.isPresent()
        ? Optional.of(
        FluentIterable.from(referenceRanges.get()).transform(
            new Function<Range, String>() {
              @Override
              public String apply(Range range) {
                return range.getReferenceName();
              }
            }).toSet())
        : Optional.<Set<String>>absent();

    List<Reference> references = Lists.newArrayList();
    for (String referenceId : referenceSet.getReferenceIds()) {
      //TODO: batch up the reference lookups.
      Reference ref = genomics.references().get(referenceId).execute();
      if (!referenceNames.isPresent() || referenceNames.get().contains(ref.getName())) {
        references.add(ref);
      }
    }
    return references;
  }

  private static Iterable<Range> getShards(Iterable<Range> inputRanges, long numberOfBasesPerShard) {
    List<Range> shards = Lists.newArrayList();
    for (Range inputRange : inputRanges) {
      long end = inputRange.getEnd();
      long start = inputRange.getStart();
      double shardCount = Math.ceil(end - start) / (double) numberOfBasesPerShard;
      for (int i = 0; i < shardCount; i++) {
        long shardStart = start + (i * numberOfBasesPerShard);
        long shardEnd = Math.min(end, shardStart + numberOfBasesPerShard);
        Range range = inputRange.clone();
        range.setStart(shardStart);
        range.setEnd(shardEnd);
        shards.add(range);
      }
    }
    Collections.shuffle(shards); // Shuffle shards for better backend performance
    return shards;
  }
}
