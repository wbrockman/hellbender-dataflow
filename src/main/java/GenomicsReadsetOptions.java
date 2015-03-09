import com.google.cloud.genomics.dataflow.utils.GenomicsDatasetOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
/**
 * Created by brockman on 3/2/15.
 */
public interface GenomicsReadsetOptions extends GenomicsDatasetOptions {
  @Description("The ID of the Google Genomics ReadGroupSet this pipeline is working with. ")
  @Default.String("")
  String getReadGroupSetId();
}
