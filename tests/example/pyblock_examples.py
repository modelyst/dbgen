#   Copyright 2021 Modelyst LLC
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

from compgraph import Dataset, Env, Import, PyNode, TransformedDataset, TransformGraph, node
from libraries.santosh.tranforms import incredible_transform
from libraries.steven.transforms import amazing_transform

dataset_node = Dataset(level='datapoint')

# define transform
@node
def add_one(data_point):
    return data_point + 1


# turn into node
add_one_node = PyNode(function=add_one, inputs=[dataset_node['out']], outputs=['add_out'])

# Compose imports into graph
amazing_env = Env([Import('scipy')])
amazing_node = PyNode(
    function=amazing_transform, env=amazing_env, inputs=[add_one_node['out']], outputs=['amazing_out']
)

# add another node
incredible_node = PyNode(
    function=amazing_transform, inputs=[amazing_node['incredible_out']], outputs=['incredible_out']
)

# dump outputs into a key in the new dataset
tf_dataset = TransformedDataset(
    my_transformed_key=incredible_transform['incredible_out'], key_2=add_one_node['add_out']
)

# Add nodes to Graph
graph = TransformGraph(
    name='stevens_transform',
    dataset=dataset_node,
    nodes=[incredible_node, amazing_node, add_one_node],
    transformed_dataset=tf_dataset,
)


if __name__ == '__main__':
    graph.push()
