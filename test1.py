import boto3
from django.utils import timezone
from datetime import datetime
import pandas as pd
from io import StringIO

default_args = {

    'account': '1234567890',
    'line_item_product_code1':'AmazonEC2',
    'line_item_product_code2':'AmazonEMRCluster'
}

bucket_name = "listbucket-testing"
all_regions = []
year,month,date,acc_id,prod_code,box_usage,az,res_id,res_type,res_region,total_up_time,all_regions,num_instances,cluster_ins_type ,cluster_instances_running_count,emr_box_usage,emr_box_usage_list,emr_box_usage_list,ec2_instance_list,cluster_ins_id= ([] for i in range(20))
ec2_client = boto3.client("ec2",region_name = "us-east-1")
fi_EC2 = [{'Name':'instance-state-name', 'Values':["running"]}]


"""
   Collect ll the regions and insert into  "all_regions" list
"""
def Get_All_Regions():
    for region in ec2_client.describe_regions()['Regions']:
        all_regions.append(region["RegionName"])
    return()
    
"""
 Create dataframe containing all of the resources's metadata, convert dataframe into
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
    csv_buffer = StringIO()
    resource_dataframe.to_csv(csv_buffer, index = False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_name, 'ListResources.csv').put(Body=csv_buffer.getvalue())
    print("**************** report generation done *****************")
    return()
    
# =========== start =========== EC2 related functions ============================   
"""
  Total up-time calculation of individual resources
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
     prod_code.append(default_args["line_item_product_code1"])
     box_usage.append(("BoxUsage:"+each_ec2_instance["Instances"][0]["InstanceType"]))
     az.append(each_ec2_instance["Instances"][0]["Placement"]["AvailabilityZone"])
     res_id.append(each_ec2_instance["Instances"][0]["InstanceId"])
     res_type.append(each_ec2_instance["Instances"][0]["InstanceType"])
     res_region.append(each_region)
     num_instances.append(1)
     Calculate_Ec2_Up_Time(each_ec2_instance["Instances"][0]["LaunchTime"])
     return()
     
# =========== end =========== EC2 related functions ==============================       
     
# =========== start ========= EMR cluster related functions =======================     
    
"""
 Finds Cluster instance details like instance type  
"""   

def Cluster_Instance_Fleet_Details(cluster_id):
    instance_fleet_response = emr_conn.list_instance_fleets(ClusterId = str(cluster_id))

    for each in instance_fleet_response["InstanceFleets"]:
        fleet_type  = each["InstanceFleetType"]
        fleet_ins_type = each["InstanceTypeSpecifications"][0]["InstanceType"]
        ins_type = fleet_type + ": " + fleet_ins_type
        cluster_ins_type.append(ins_type)
        emr_box_usage.append(fleet_type +"_BoxUsage:"+ fleet_ins_type)
        print("this is test")
    ins_type_set = set(cluster_ins_type)
    ins_type_list = list(ins_type_set)
    emr_box_usage_set = set(emr_box_usage)
    emr_box_usage_list = list(emr_box_usage_set)
    box_usage.append(emr_box_usage_list)
    res_type.append(ins_type_list)
    return()
    
"""
Creates a list of Ec2 instances related EMR clusters only and counts total no of instances
for a given cluster
"""

def Cluster_List_Instance_Details():
    list_instance_response = emr_conn.list_instances(ClusterId = cluster_id )
    cluster_instances_running_count = len(list_instance_response["Instances"])
    num_instances.append(cluster_instances_running_count)
    for each_ins in list_instance_response["Instances"]:
        cluster_ins_id.append(each_ins["Ec2InstanceId"])
    return() 
    
# ============ end =========== EMR cluster related functions ====================== 
"""
Gathering informations for EC2 instances
"""
Get_All_Regions()
for each_region in all_regions:
        conn = boto3.client('ec2',region_name=each_region)
        ec2_response = conn.describe_instances(Filters=fi_EC2)["Reservations"]
        for each_ec2_instance in ec2_response:
            if each_ec2_instance["Instances"][0]["InstanceId"] not in cluster_ins_id :  #Exclude cluster instance
                Collect_Ec2_Data()
                ec2_instance_list = res_id   #hold running ec2 instances
print("ec2 done")

"""
Gathering informations for EMR clusters
"""

cluster_instance_count = 0
for each_region in all_regions:
    emr_conn = boto3.client('emr',region_name=each_region)
    emr_response = emr_conn.list_clusters()
    if len(emr_response["Clusters"]) != 0:
        for each_cluster in emr_response["Clusters"]:
            cluster_launh_time = each_cluster["Status"]["Timeline"]["CreationDateTime"]
            year.append(cluster_launh_time.year)
            month.append(cluster_launh_time.month)
            date.append(cluster_launh_time.day)
            acc_id.append(default_args["account"])
            prod_code.append(default_args["line_item_product_code2"])
            cluster_id = each_cluster["Id"]
            Cluster_Instance_Fleet_Details(cluster_id)
            cluster_response = emr_conn.describe_cluster(ClusterId = str(cluster_id))
            az.append(cluster_response["Cluster"]["Ec2InstanceAttributes"]["Ec2AvailabilityZone"])
            res_id.append(cluster_id)
            res_region.append(each_region)
            num_instances.append(cluster_instances_running_count)
            cluster_run_time = emr_response["Clusters"][0]["NormalizedInstanceHours"]
            total_up_time.append(cluster_run_time)

print("emr done")

Generate_Report()
print("successfull completion. Please check " + bucket_name + " for results")



