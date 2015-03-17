import com.google.api.client.util.Maps;
import com.google.api.services.genomics.Genomics;
import com.google.api.services.genomics.model.*;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.genomics.dataflow.readers.GenomicsApiReader;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import com.google.cloud.genomics.dataflow.utils.GenomicsDatasetOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.Paginator;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.*;
import java.util.logging.Logger;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Created by brockman on 3/1/15.
 */
public class SampleReadPipeline {
  private static final Logger LOG = Logger.getLogger(SampleReadPipeline.class.getName());

  public interface GenomicsReadsetOptions extends GenomicsDatasetOptions {
    @Description("The ID of the Google Genomics ReadGroupSet this pipeline is working with. ")
    @Default.String("")
    String getReadGroupSetId();
    void setReadGroupSetId(String rgsid);
  }

  public static void main(String[] args) throws IOException, GeneralSecurityException {
    GenomicsReadsetOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation().as(GenomicsReadsetOptions.class);
    GenomicsOptions.Methods.validateOptions(options);

    GenomicsFactory.OfflineAuth auth = GenomicsOptions.Methods.getGenomicsAuth(options);
    Genomics genomics = auth.getGenomics(auth.getDefaultFactory());

    // Input ranges and references
    String readGroupSetId = options.getReadGroupSetId();
    ReadGroupSet readGroupSet = genomics.readgroupsets().get(readGroupSetId).execute();
    String referenceSetId = readGroupSet.getReferenceSetId();
    ReferenceSet referenceSet = genomics.referencesets().get(referenceSetId).execute();
    Iterable<Reference> references;
    Iterable<Range> inputRanges;
    if (options.isAllReferences()) {
      LOG.info("Processing all reference contigs.");
      references = lookupReferences(genomics, referenceSet, Optional.<Iterable<Range>>absent()); // TODO use Java8 optional
      inputRanges = FluentIterable.from(references).transform(reference ->
        new Range().setReferenceName(reference.getName()).setStart(0L).setEnd(reference.getLength()));
    } else {
      LOG.info("Processing reference contigs in options: " + options.getReferences());
      inputRanges = parseRangesFromCommandLine(options.getReferences());
      references = lookupReferences(genomics, referenceSet, Optional.of(inputRanges));
    }
    Map<String, String> refNameToId = getRefNameToIdMap(references);

    // Select shard size to work on
    List<Range> ranges = getShards(inputRanges, 100000);

    // Start constructing the pipeline
    Pipeline p = Pipeline.create(options);
    DataflowWorkarounds.registerGenomicsCoders(p);

    // Pipeline begins with numWorkers PCollections, each having an equal share of the input shards
    int numWorkers = Math.min(ranges.size(), options.getNumWorkers());
    LOG.info("Turning " + ranges.size() + " ranges into " + numWorkers + " workers");
    int optionsPerWorker = (int) Math.ceil(ranges.size() / numWorkers);
    List<PCollection<Range>> pCollections = Lists.newArrayList();
    for (int start = 0; start < ranges.size(); start += optionsPerWorker) {
      int end = start + optionsPerWorker;
      LOG.info("Adding collection with " + start + " to " + end);
      pCollections.add(p.begin().apply(Create.of(ranges.subList(start, end))));
    }

    // Flatten the list of PCollections into a single PCollection.
    PCollectionList<Range> rangesByWorker = PCollectionList.<Range>of(pCollections);
    PCollection<Range> rangeCollection = rangesByWorker.apply(Flatten.<Range>pCollections());

    // Read the reads and the reference bases, keying each by the Range.
    PCollection<KV<Range, Read>> reads = rangeCollection.apply(ParDo.of(new ReadReader(auth, null, readGroupSetId)));
    PCollection<KV<Range, String>> refBases = rangeCollection.apply(ParDo.of(new RefBasesReader(auth, refNameToId)));

    // Join the two collections to enable working with both reads and reference together.
    final TupleTag<String> refTag = new TupleTag<>("refTag");
    final TupleTag<Read> readTag = new TupleTag<>("readTag");
    PCollection<KV<Range, CoGbkResult>> refAndReads =
        KeyedPCollectionTuple.of(refTag, refBases)
            .and(readTag, reads)
            .apply(CoGroupByKey.<Range>create());

    // Print the read alignments to reference, and write them out.
    PCollection<String> printed = refAndReads.apply(ParDo.of(new ReadPrinter(refTag, readTag)));
    printed.apply(TextIO.Write.to(options.getOutput()));

    p.run();
  }

  private static Map<String, String> getRefNameToIdMap(Iterable<Reference> references) {
    Map<String, String> map = Maps.newHashMap();
    for (Reference ref : references) {
      map.put(ref.getName(), ref.getId());
    }
    return map;
  }

  private static class ReadPrinter
      extends com.google.cloud.dataflow.sdk.transforms.DoFn<KV<Range, CoGbkResult>, String> {
    private static final Logger LOG = Logger.getLogger(ReadPrinter.class.getName());
    private final TupleTag<String> refTag;
    private final TupleTag<Read> readTag;

    public ReadPrinter(TupleTag<String> refTag, TupleTag<Read> readTag) {
      this.refTag = refTag;
      this.readTag = readTag;
    }

    @Override
    public void processElement(ProcessContext processContext) throws Exception {
      KV<Range, CoGbkResult> input = processContext.element();
      Range range = input.getKey();
      String refBases = input.getValue().getOnly(refTag);
      LOG.info("Processing range " + range.toString() + " Ref length " + refBases.length());
      Iterable<Read> reads = input.getValue().getAll(readTag);
      for (Read read : reads) {
        StringBuilder out = new StringBuilder(read.getAlignedSequence().length());
        Long offset = (long) 0;
        for (CigarUnit c : read.getAlignment().getCigar()) {
          switch (c.getOperation()) {
            case "ALIGNMENT_MATCH":
            case "SEQUENCE_MATCH":
            case "SEQUENCE_MISMATCH":
              int start = offset.intValue();
              offset += c.getOperationLength();
              out.append(read.getAlignedSequence().substring(start, offset.intValue()));
              break;
            case "CLIP_SOFT":
            case "INSERT":
              offset += c.getOperationLength();
              break;
            case "PAD":
              repeat(out, '*', c.getOperationLength());
              break;
            case "DELETE":
              repeat(out, '-', c.getOperationLength());
              break;
            case "SKIP":
              repeat(out, ' ', c.getOperationLength());
              break;
            case "CLIP_HARD":
              break;
          }
        }
        Long refOffset = read.getAlignment().getPosition().getPosition() - range.getStart();

        processContext.output("Read@" + refOffset + ": " + out.toString());
        processContext.output("Ref @" + refOffset + ": " + refBases.substring(
            refOffset.intValue(),
            Math.min(refBases.length(), refOffset.intValue() + read.getAlignedSequence().length())));
      }
    }

    private void repeat(StringBuilder out, char symbol, long copies) {
      for (int i = 0; i < copies; i++) {
        out.append(symbol);
      }
    }
  }

  private static class ReadReader extends GenomicsApiReader<Range, KV<Range, Read>> {
    private static final Logger LOG = Logger.getLogger(ReadReader.class.getName());
    private static String readGroupSetId;
    /**
     * Create a ReadReader using a auth and fields parameter. All fields not specified under
     * readFields will not be returned in the API response.
     *
     * @param auth Auth class containing credentials.
     * @param readFields Fields to return in responses.
     * @param readGroupSetId read group set to work on.
     */
    public ReadReader(GenomicsFactory.OfflineAuth auth, String readFields, String readGroupSetId) {
      super(auth, readFields);
      this.readGroupSetId = readGroupSetId;
    }

    @Override
    protected void processApiCall(Genomics genomics, ProcessContext c, Range range)
        throws IOException {
      LOG.info("Starting Reads read loop");
      SearchReadsRequest request = new SearchReadsRequest()
          .setReadGroupSetIds(Collections.singletonList(readGroupSetId))
          .setReferenceName(range.getReferenceName())
          .setStart(range.getStart())
          .setEnd(range.getEnd());
      for (Read read : Paginator.Reads.create(genomics, Paginator.ShardBoundary.STRICT).search(request, fields)) {
        c.output(KV.of(range, read));
      }
    }
  }

  public static class RefBasesReader extends GenomicsApiReader<Range, KV<Range, String>> {
    private static final Logger LOG = Logger.getLogger(RefBasesReader.class.getName());
    private static Map<String, String> refNameToId;
    /**
     * Create a RefBasesReader with no fields parameter: all information will be returned.
     * @param auth Auth class containing credentials.
     */
    public RefBasesReader(GenomicsFactory.OfflineAuth auth, Map<String, String> refNameToId) {
      super(auth, null);
      this.refNameToId = refNameToId;
    }

    @Override
    protected void processApiCall(Genomics genomics, ProcessContext c, Range range)
        throws IOException {
      LOG.info("Reading Reference bases: id " + range.getReferenceName() + " start "
          + range.getStart() + " end " + range.getEnd());
      Genomics.References.Bases.List listRequest =
          genomics.references().bases().list(refNameToId.get(range.getReferenceName()));
      listRequest.setStart(range.getStart());
      listRequest.setEnd(range.getEnd());
      final ListBasesResponse result = listRequest.execute();
      c.output(KV.of(range, result.getSequence()));
    }
  }

  private static Iterable<Range> parseRangesFromCommandLine(String rangeOptions) {
    return FluentIterable.from(Splitter.on(",").split(rangeOptions)).transform(rangeString -> {
      ArrayList<String> rangeInfo = newArrayList(Splitter.on(":").split(rangeString));
      return new Range().setReferenceName(rangeInfo.get(0))
          .setStart(Long.valueOf(rangeInfo.get(1)))
          .setEnd(Long.valueOf(rangeInfo.get(2)));
    });
  }

  private static Iterable<Reference> lookupReferences(
      Genomics genomics, ReferenceSet referenceSet, Optional<Iterable<Range>> referenceRanges) throws IOException {
    Optional<? extends Set<String>> referenceNames = referenceRanges.isPresent()
        ? Optional.of(
        FluentIterable.from(referenceRanges.get()).transform(range -> range.getReferenceName()).toSet())
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

  private static List<Range> getShards(Iterable<Range> inputRanges, long numberOfBasesPerShard) {
    List<Range> shards = Lists.newArrayList();
    for (Range inputRange : inputRanges) {
      long end = inputRange.getEnd();
      long start = inputRange.getStart();
      int shardCount = 0;
      for (long shardStart = start; shardStart < end; shardStart += numberOfBasesPerShard, ++shardCount) {
        long shardEnd = Math.min(end, shardStart + numberOfBasesPerShard);
        Range range = inputRange.clone();
        range.setStart(shardStart);
        range.setEnd(shardEnd);
        shards.add(range);
      }
      LOG.info("Sharded range: " + inputRange.toString() + " into " + shardCount + " shards");
    }
    Collections.shuffle(shards); // TODO - is this still good practice? Shuffle shards for better backend performance
    return shards;
  }
}
