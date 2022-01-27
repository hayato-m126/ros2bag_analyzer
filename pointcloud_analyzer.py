import argparse
import csv
import glob
import os
from pathlib import Path

import yaml
from natsort import natsorted


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("bag_directory", help="The root directory where the bags to be analyzed are located.")
    parser.add_argument("-r", "--rate", default="0.5", help="ros bag play rate")
    args = parser.parse_args()

    regex = os.path.join(os.path.expandvars(args.bag_directory), "**", "metadata.yaml")
    output_csv_path = os.path.join(
        os.path.expandvars(args.bag_directory), "analyze.csv"
    )
    with open(output_csv_path, "a") as output_csv_file:
        writer = csv.writer(output_csv_file)
        writer.writerow(
            [
                "bag_file",
                "duration",
                "velodyne_packets",
                "concat_pcd",
                "map_filtered_pcd",
                "no_ground_pcd",
            ]
        )
        yaml_paths = glob.glob(regex, recursive=True)
        for yaml_path_str in natsorted(yaml_paths):
            with open(yaml_path_str) as metadata_yaml_file:
                yaml_obj = yaml.load(metadata_yaml_file, Loader=yaml.SafeLoader)
                yaml_top = yaml_obj["rosbag2_bagfile_information"]
                bag_file = Path(yaml_path_str).parent.name
                duration = yaml_top["duration"]["nanoseconds"] / pow(10, 9) * args.rate
                c_velodyne = 0
                c_concat = 0
                c_map_filtered = 0
                c_no_ground = 0
                for topic in yaml_top["topics_with_message_count"]:
                    if (
                        topic["topic_metadata"]["name"]
                        == "/sensing/lidar/top/velodyne_packets"
                    ):
                        c_velodyne = topic["message_count"]
                    if (
                        topic["topic_metadata"]["name"]
                        == "/sensing/lidar/concatenated/pointcloud"
                    ):
                        c_concat = topic["message_count"]
                    if (
                        topic["topic_metadata"]["name"]
                        == "/sensing/lidar/map_filtered/pointcloud"
                    ):
                        c_map_filtered = topic["message_count"]
                    if (
                        topic["topic_metadata"]["name"]
                        == "/sensing/lidar/no_ground/pointcloud"
                    ):
                        c_no_ground = topic["message_count"]
                writer.writerow(
                    [
                        bag_file,
                        duration,
                        c_velodyne,
                        c_concat,
                        c_map_filtered,
                        c_no_ground,
                    ]
                )


if __name__ == "__main__":
    main()
