# Copyright 2016 - Nokia
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

# import Crypto.Hash.MD5


from rca4tracing.common.utils import cal_md5


class PropertiesElement(object):

    def __init__(self, properties=None):
        if properties is None:
            properties = {}
        self.properties = properties

    def __getitem__(self, key):
        """Get a property with 'value = element[key]'"""
        return self.properties[key]

    def __setitem__(self, key, value):
        self.properties[key] = value

    def __delitem__(self, key):
        """Delete a property with 'del(element[key])"""
        if key in self.properties:
            del self.properties[key]

    def __iter__(self):
        return self.properties.values()

    def get(self, k, d=None):
        return self.properties.get(k, d)

    def items(self):
        return self.properties.items()

    def copy(self):
        return PropertiesElement(self.properties.copy())


class Vertex(PropertiesElement):
    """Class Vertex

    A vertex is defined as follows:
    * vertex_id is a unique identifier
    * properties is a dictionary

    """

    def __init__(self, vertex_id, properties=None, label=None):
        """Create a Vertex instance

            :type vertex_id: str
            :type properties: dict
            :rtype: Vertex
            """
        super(Vertex, self).__init__(properties)
        if vertex_id is None:
            raise AttributeError('Attribute vertex_id is missing')
        if not label:
            self.label = label
        self.vertex_id = vertex_id

    def __hash__(self):
        obj_str = '%s' % (self.vertex_id)
        digester = Crypto.Hash.MD5.new()
        digester.update(obj_str.encode('utf-8'))
        return int(digester.hexdigest(), 16)

    def __repr__(self):
        return '{vertex_id : %s, properties : %s}' % \
               (self.vertex_id, self.properties)

    def __eq__(self, other):
        """Compare two vertices

        Example
        -------
        if vertex1 == vertex2:
            do something

        :type other: Vertex
        :rtype: bool
        """
        return vars(self) == vars(other) and \
            self.properties == other.properties and \
                self.label == other.label

    def copy(self):
        return Vertex(vertex_id=self.vertex_id,
                      properties=self.properties.copy(),
                      label=label)


class Edge(PropertiesElement):
    """Class Edge represents a directional edge between two vertices

    An edge is defined as follows:
    * source_id is the first vertex id
    * target_id is the second vertex id
    * properties is a dictionary

    +---------------+    edge     +---------------+
    | source vertex |-----------> | target vertex |
    +---------------+             +---------------+

    """

    def __init__(self, source_id, target_id, label, properties=None):
        """Create an Edge instance

        :param source_id: source vertex id
        :type source_id: str

        :param target_id: target vertex id`
        :type target_id: str

        :param label:
        :type label: str

        :type properties: dict
        :rtype: Edge
        """
        super(Edge, self).__init__(properties)
        if not source_id:
            raise AttributeError('Attribute source_id is missing')
        if not target_id:
            raise AttributeError('Attribute target_id is missing')
        if not label:
            raise AttributeError('Attribute label is missing')
        self.source_id = source_id
        self.target_id = target_id
        self.label = label

    def __hash__(self):
        obj_str = '%s%s%s' % (self.source_id, self.target_id, self.label)
        # import Crypto.Hash.MD5
        # digester = Crypto.Hash.MD5.new()
        # digester.update(obj_str.encode('utf-8'))
        # return int(digester.hexdigest(), 16)
        return int(cal_md5(obj_str.encode('utf-8')), 16)

    def __repr__(self):
        return '{source_id : %s, target_id : %s, ' \
               'label = %s, properties : %s}' % (self.source_id,
                                                 self.target_id,
                                                 self.label,
                                                 self.properties)

    def __eq__(self, other):
        """Compare two edges

        Example
        -------
        if edge1 == edge2:
            do something

        :type other: Edge
        :rtype: bool
        """
        return vars(self) == vars(other) and \
            self.properties == other.properties

    def other_vertex(self, v_id):
        """If v_id == target_id return source_id, else return target_id

        :param v_id: the vertex id
        :return: the other vertex id
        """
        return self.source_id if self.target_id == v_id else self.target_id

    def has_vertex(self, v_id):
        return self.source_id == v_id or self.target_id == v_id

    def copy(self):
        return Edge(source_id=self.source_id,
                    target_id=self.target_id,
                    label=self.label,
                    properties=self.properties.copy())


if __name__ == '__main__':
    e = Edge('1', '2', 'p')
    v = Vertex('100')
    print(hash(e))
    print(hash(v))
