package utilities;

import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.regions.Regions;


public class Names {

    public static final String BUCKET = "s3://yaad-nitzan-hypernym-bucket/";
    public static final String NAME = "yaad-nitzan-hypernym-bucket";

    public static final String KEY = "Yaad-Nitzan-Key";
    public static final String INSTANCE_TYPE = InstanceType.M4Large.toString();
    public static final Regions REGION = Regions.US_EAST_1;
}
