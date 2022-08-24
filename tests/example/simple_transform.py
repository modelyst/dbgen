#   Copyright 2022 Modelyst LLC
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

"""Simple Transform Proposal"""
from dataset_transformer import Dataset, PyNode, TransformedDataset, TransformGraph

# Define a dataset to pull from
dataset_node = Dataset(level='datapoint')

# define transform
def transformer(data_point):
    return data_point + 1


# turn into node
transform_node = PyNode(function=transformer, inputs=[dataset_node['out']], outputs=['transformed_out'])

# add data to the dataset
tf_dataset = TransformedDataset(key_1=transform_node['transformed_out'])

# Add nodes to Graph
graph = TransformGraph(
    name='simple_transform',
    dataset=dataset_node,
    nodes=[transform_node],
    trasnformed_dataset=tf_dataset,
)


if __name__ == '__main__':
    # Push to the database
    graph.push()

    # Run the Transform
    transformed_dataset = graph.run(dataset_name='stevens_dataset')
