import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class config {

    public static StepConfig stepConfig (String name, HadoopJarStepConfig jar, String action) {
        return new StepConfig()
                .withName(name)
                .withHadoopJarStep(jar)
                .withActionOnFailure(action);
    }

    public static HadoopJarStepConfig hadoopStepConfig (String jar, String[] args) {
        return new HadoopJarStepConfig()
                .withJar(jar)
                .withArgs(args);
    }

}
