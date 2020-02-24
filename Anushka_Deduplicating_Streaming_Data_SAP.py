""" TAKE HOME ASSIGNMENT - SAP """

"""
Question: Remove duplicates from streaming JSON data 
Assumptions:  
1. Original data is in the form of JSON objects. 
2. Assuming there is a unique ID associated with each object. 
3. Each ID can be used to retrieve the JSON object. 
"""

"#************************** METHOD 1 **************************#"
# Method 1: Using Dictionary. Every time a new ID comes in, we store it in a dictionary if it hasn't been seen before. 
# If we have seen it before then we discard it and do not add it to dictionary. 

input_data = [
    (1, '{"name":"A"}'),
    (2, '{"name":"C"}'),
    (1, '{"name":"A"}'),
    (3, '{"name":"D"}'),
    (2, '{"name":"C"}'),
    (1, '{"name":"A"}'),
    (4, '{"name":"B"}')
]
# Input is in the form of a list of tuples. 
# First index of tuple contains key and second contains value which is the json data in the form of a string.

from collections import defaultdict


def deduplication(data):
    output_data = defaultdict(str)  # Declaring output_data as dictionary with keys as unique IDs and values as JSON string
    for key, json_string in data:   
        if key not in output_data:  # For each ID(key) from the streaming data, check if we have already seen it before in our output_data dictionary.
            output_data[key] = json_string  # If we have not seen the new incoming ID before, we add the key, value pair to our output_data dictionary.
    return output_data  # Return the final output in the form of a dictionary which has no repeating values.


print(deduplication(input_data))

# Time Complexity = O(n)
# Space Complexity = O(n)
# where n is the size of the streaming data


"#************************** METHOD 2 **************************#"
# Method 2: Optimization to save on space. This method is where we implement a LRU cache and only keep the most recently used items in our cache.
# This method is proposed to counter the problem posed by huge amounts of streaming data that is coming in every second and not having enough space to store this data at run time.
# We process data within a rolling window of just the most recent records observed.

from collections import OrderedDict  # OrderedDict is offered in the collections library in Python. It preserves the order in which the keys are inserted.


class LRUCache:
    """
    LRUCache object will be instantiated and called as such:
    cache = LRUCache(capacity)
    cache.add_to_cache(key, value)
    """

    def __init__(self, capacity: int):  # Class initialization
        self.capacity = capacity  # Initialize LRUCache with a fixed capacity based on space available
        self.cache = OrderedDict()  # Initialize LRUCache with cache object (Ordered Dictionary)
    
    def add_to_cache(self, key: int, value: str):  # Value is json data of type string
        if key in self.cache:  # If key is already present in cache, then move it to the end of the queue since it is most recently referenced.
            self.cache.move_to_end(key)  # OrderedDict.move_to_end() method is used to move this particular key to the end.
        self.cache[key] = value 
        if len(self.cache) > self.capacity: 
            self.cache.popitem(last=False)  # If the cache has reached its capacity then remove the least recently used item from the cache. The popitem() method returns and removes a (key, value) pair. last=False implies that the cache is emptied in First-in-first-out manner.
        

# Time complexity = O(1)
# O(1) since all operations with ordered dictionary are done in a constant time.
# Space complexity = O(capacity)
# O(capacity) since the space is used only for an ordered dictionary. 
