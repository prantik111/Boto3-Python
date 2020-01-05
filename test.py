
import boto3
from django.utils import timezone
from datetime import datetime
import pandas as pd
from io import StringIO

account_id = boto3.client('sts').get_caller_identity()['Account']

default_args = {
    
    'account': account_id,
    'line_item_product_code1':'AmazonEC2',
    'line_item_product_code2':'AmazonEMRCluster',
    'line_item_product_code3':'K8 Cluster Ec2 Instance:',
    'line_item_product_code4':'S3'
}

bucket_name = "test"

year,month,date,acc_id,prod_code,box_usage,az,res_id,res_type,res_region,total_up_time,num_instances = ([] for i in range(12))
all_regions,cluster_ins_type ,emr_box_usage,emr_box_usage_list,cluster_ins_id,k8_cluster_lists = ([] for j in range(6))
bucket_name, bucket_creation_time = []


ec2_client = boto3.client("ec2",region_name = "us-east-1")
fi_EC2 = [{'Name':'instance-state-name', 'Values':["running"]}]

# **************************** Common functions *****  start ***********************************************

"""
   Collect ll the regions and insert into  "all_regions" list
"""
def Get_All_Regions():
    for region in ec2_client.describe_regions()['Regions']:
        all_regions.append(region["RegionName"])
    return()
      
    
"""
 Creates Panda dataframe containing all of the resources's metadata, convert dataframe into
 .csv file and export it into s3 bucket
"""
def Generate_Report():
    resources = {
                    "Year":year,"Month":month,"Date":date,"Account_id":acc_id,"Line_Item_Prod_code":prod_code,
                    "Line_Item_Usage_Type":box_usage,"Line_Item_AZ":az,"Line_Item_Resource_Id":res_id,
                    "Product_Instance_Type":res_type,"Product_Region":res_region,"Num_Instance":num_instances,
                    "Up_Time_In_Hours":total_up_time
                }
    resource_dataframe = pd.DataFrame(resources)
    resource_dataframe_sorted = resource_dataframe.sort_values("Up_Time_In_Hours",ascending = False)
    csv_buffer = StringIO()
    resource_dataframe_sorted.to_csv(csv_buffer, index = False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_name, 'ListResources.csv').put(Body=csv_buffer.getvalue())
    return()
    
    
# =========== start =========== EC2 related functions =====================================================

"""
  Total up-time calculation of individual EC2 instances
"""
def Calculate_Ec2_Up_Time(launch_time):
    now = datetime.now(timezone.utc)
    diff = now - launch_time
    UpTime_InHours = round(((diff.total_seconds())/3600), 3)
    total_up_time.append(UpTime_InHours)
    return()
    

"""
   Inserting Ec2 Instance metadata into corresponding lists
"""
def Collect_Ec2_Data():
     year.append(each_ec2_instance["Instances"][0]["LaunchTime"].year)
     month.append(each_ec2_instance["Instances"][0]["LaunchTime"].month)
     date.append(each_ec2_instance["Instances"][0]["LaunchTime"].day)
     acc_id.append(default_args["account"])
     box_usage.append(("BoxUsage:"+each_ec2_instance["Instances"][0]["InstanceType"]))
     az.append(each_ec2_instance["Instances"][0]["Placement"]["AvailabilityZone"])
     res_id.append(each_ec2_instance["Instances"][0]["InstanceId"])
     res_type.append(each_ec2_instance["Instances"][0]["InstanceType"])
     res_region.append(each_region)
     num_instances.append("1")
     Calculate_Ec2_Up_Time(each_ec2_instance["Instances"][0]["LaunchTime"])
     return()
     
# =========== end =========== EC2 related functions =======================================================      
     
# =========== start ========= EMR cluster related functions ===============================================    
 
"""
 Finds Cluster instance details like instance type(Master and Core) and total number of instances for a given 
 EMR cluster
"""   

def Cluster_Instance_Fleet_Details(cluster_id):
    instance_fleet_response = emr_conn.list_instance_fleets(ClusterId = str(cluster_id))
    capacity=[]
    for each in instance_fleet_response["InstanceFleets"]:
        fleet_type  = each["InstanceFleetType"]
        fleet_ins_type = each["InstanceTypeSpecifications"][0]["InstanceType"]
        ins_type = fleet_type + ": " + fleet_ins_type
        cluster_ins_type.append(ins_type)
        emr_box_usage.append(fleet_type +"_BoxUsage:"+ fleet_ins_type)
        if each["Name"].startswith("Core"):
            capacity.append(each["TargetSpotCapacity"])
        elif each["Name"].startswith("Master"):
            capacity.append(each["TargetOnDemandCapacity"])
            
    ins_type_set = set(cluster_ins_type)
    ins_type_list = list(ins_type_set)
    emr_box_usage_set = set(emr_box_usage)
    emr_box_usage_list = list(emr_box_usage_set)
    box_usage.append(emr_box_usage_list)
    res_type.append(ins_type_list)
    total_emr_instances = sum(capacity)
    num_instances.append(total_emr_instances)
    return ()
    
"""
Creates a list of Ec2 instances related to EMR clusters only 
"""

def Cluster_List_Instance_Details(cluster_id):
    list_instance_response = emr_conn.list_instances(ClusterId = cluster_id)
    for each_ins in list_instance_response["Instances"]:
        cluster_ins_id.append(each_ins["Ec2InstanceId"])
    return() 
  
# ============ end =========== EMR cluster related functions ===============================================

    
"""
Differentiate normal ec2 instances from K8 cluster instances based on instance "Name" tag value. Every K8 cluster 
instances "Name" tag value includes the corresponding K8 cluster name. If the value doesnot contain the cluster 
name or no tag is associated with the instance then the instance is considered as a normal Ec2 instance.
"""
   
def Fetch_Prod_des(tag_resposne):
    if len(tag_resposne["Tags"])== 0:
        prod_code.append(default_args["line_item_product_code1"])
        return()
    elif len(tag_resposne["Tags"])!= 0:
        name_tag_value = tag_resposne["Tags"][0]["Value"]
        for each_k8_cluster in k8_cluster_lists:
            if each_k8_cluster in name_tag_value:
                k8_prod_code = default_args["line_item_product_code3"] + each_k8_cluster 
                prod_code.append(k8_prod_code)
                return()
        prod_code.append(default_args["line_item_product_code1"])
        return()

# ============ start  =========== S3 related functions =====================================================


def Capture_S3_details:
	for each_region in all_regions:
		s3_client = boto3.client('s3',region_name = each_region) 
		s3_response = s3_client.list_buckets()
		for buckets in s3_response["Buckets"]:

			bucket_creation_time = " "
			bucket_name = " "
			bucket_size_in_GB = 0
			bucket_name = (buckets["Name"])
			bucket_creation_time = buckets["CreationDate"]

			year.append(bucket_creation_time.year)
			month.append(bucket_creation_time.month)
            date.append(bucket_creation_time.day)
            acc_id.append(default_args["account"])
            prod_code.append(default_args["line_item_product_code4"])
            res_id.append(bucket_name)
            res_region.append(each_region)
            num_instances.append("1")
            bucket_size_in_GB = sum([object.size for object in boto3.resource('s3').Bucket('mybucket').objects.all()])
            total_up_time.append(bucket_size_in_GB)

            emr_box_usage_list.append(" ")     # N/A For S3 buckets, adding empty items.
            Line_Item_Usage_Type.append(" ")   # N/A For S3 buckets, adding empty items.
            Line_Item_AZ.append(" ")           # N/A For S3 buckets, adding empty items.

            return()

# ============ end =========== S3 related functions ========================================================


# **************************** Common functions ******  end ************************************************ 

"""
Gathering informations for EMR clusters and corresponding instances
"""

Get_All_Regions()
cluster_instance_count = 0
for each_region in all_regions:
    emr_conn = boto3.client('emr',region_name=each_region)
    emr_response = emr_conn.list_clusters()
    if len(emr_response["Clusters"]) != 0:
        for each_emr_cluster in emr_response["Clusters"]:
            cluster_launh_time = each_emr_cluster["Status"]["Timeline"]["CreationDateTime"]
            year.append(cluster_launh_time.year)
            month.append(cluster_launh_time.month)
            date.append(cluster_launh_time.day)
            acc_id.append(default_args["account"])
            prod_code.append(default_args["line_item_product_code2"])
            cluster_id = each_emr_cluster["Id"]
            Cluster_Instance_Fleet_Details(cluster_id)       # collect EMR instance type(Master and core) and total no EMR instances
            Cluster_List_Instance_Details(cluster_id)        # collect all EMR Cluster instances
            cluster_response = emr_conn.describe_cluster(ClusterId = str(cluster_id))
            az.append(cluster_response["Cluster"]["Ec2InstanceAttributes"]["Ec2AvailabilityZone"])
            res_id.append(cluster_id)
            res_region.append(each_region)
            cluster_run_time = emr_response["Clusters"][0]["NormalizedInstanceHours"]
            total_up_time.append(cluster_run_time)


print("emr done")  

"""
Collect all available K8 cluster and store it into "k8_cluster_lists"
"""
all_regions.remove('us-west-1')  #eks is not available in this region

for each_k8_region in all_regions:
    k8_conn = boto3.client("eks",region_name = each_k8_region)
    cluster_response = k8_conn.list_clusters()
    k8_cluster_lists = k8_cluster_lists + cluster_response["clusters"]


all_regions = []
Get_All_Regions()
for each_region in all_regions:
        ec2_conn = boto3.client('ec2',region_name=each_region)
        ec2_response = ec2_conn.describe_instances(Filters=fi_EC2)["Reservations"]  #take only running instances
        for each_ec2_instance in ec2_response:
            ec2_id = ""
            ec2_id = each_ec2_instance["Instances"][0]["InstanceId"]
            if ec2_id not in cluster_ins_id:   #Exclude EMR cluster instance
                Collect_Ec2_Data()
                tag_resposne = ec2_conn.describe_tags(Filters=[{'Name':'resource-id','Values':[ec2_id]}])
                Fetch_Prod_des(tag_resposne)

print("ec2 done and K8 done")
Generate_Report()

print("successful completion. Please check " + bucket_name + " for results")
