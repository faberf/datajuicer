import operator
from collections import OrderedDict
import time
import operator

class Wrapper:
    def __init__(self, op, first, decorator):
        self.op = decorator(op)
        self.first = first
    
    def __call__(self, *args, **kwargs):
        return self.op(self.first, *args, **kwargs)

class WithOperators:

    def __init__(self, decorator):
        for key, op in operator.__dict__.items():
            if key[0] != "_" and not key in self.__dict__:
                
                wrapper = Wrapper(op, self, decorator)
                self.__dict__[key] = wrapper

    def __add__(self, *args, **kwargs):
        return self.add(*args, **kwargs)
    
    def __mul__(self, *args, **kwargs):
        return self.mul(*args, **kwargs)
    
    def __sub__(self, *args, **kwargs):
        return self.sub(*args, **kwargs)
    
    def __truediv__(self, *args, **kwargs):
        return self.truediv(*args, **kwargs)
    
    def __eq__(self, *args, **kwargs):
        return self.eq(*args, **kwargs)
    def __ne__(self, *args, **kwargs):
        return self.ne(*args, **kwargs)
    def __gt__(self, *args, **kwargs):
        return self.gt(*args, **kwargs)
    def __lt__(self, *args, **kwargs):
        return self.lt(*args, **kwargs)
    

        

class Undefined:
    pass

class Unindexed:
    pass

class Dict:
    def __init__(self, children):
        self.children = OrderedDict()
        for key in sorted(children, key=lambda key: hash(key)):
            self.children[key] = children[key]
    
    def copy(self):
        return type(self)({key:child.copy() for key,child in self.children.items()})

    def collapse(self, axis_name, coord):
        collapsed = {}
        for key, child in self.children.items():
            collapsed_child = child.collapse(axis_name, coord)
            if collapsed_child is Undefined:
                return Undefined
            collapsed[key] = collapsed_child
        return type(self)(collapsed)
    
    def get_coords(self, axis_name):
        l = [child.get_coords(axis_name) for child in self.children.values()]
        if len(l)==0:
            return set([])
        return set.union(*l)
    
    def transpose(self, *axis_names, normalize=False):
        if normalize:
            axis_names = (*axis_names, *sorted(self.get_axis_names().difference(set(axis_names))))
        if len(axis_names) == 0:
            return self.copy()
        axis_name = axis_names[-1]
        coords = set(self.get_coords(axis_name))
        
        projections = {}
        for coord in sorted(coords, key = lambda coord:  hash(coord)):
            proj = self.collapse(axis_name, coord)
            if proj is Undefined:
                continue
            projections[coord] = proj

        return Axis(axis_name, projections).balance().transpose(*axis_names[:-1])
    
    def iterate(self):
        yield self
    
    def raw(self):
        return {key:child.raw() for key,child in self.children.items()}
    
    def map(self, func, white_list =None):
        return func(self)
    
    def set(self, other):
        if type(other) is Leaf:
            return other
        new = self.copy()
        for key, child in other.children.items():
            if key in new.children:
                new.children[key] = new.children[key].set(child)
                continue
            new.children[key] = child
        return new
    
    def get_axis_names(self):
        return set(self.get_axis_data())
    
    def get_axis_data(self):
        axis_data = {}
        for child in self.children.values():
            for axis_name, data in child.get_axis_data().items():
                axis_data[axis_name]= data
        return axis_data

    def flatten(self, new_axis_name):
        return Axis.from_iterable(new_axis_name, self.iterate())
    
    def __hash__(self):
        return hash(tuple((key, hash(val)) for key,val in sorted(self.children.items())))

class IncompleteDict(Dict):
    def collapse(self, axis_name, coord):
        collapsed = {}
        for key, child in self.children.items():
            collapsed_child = child.collapse(axis_name, coord)
            if collapsed_child is Undefined:
                continue
            collapsed[key] = collapsed_child
        return IncompleteDict(collapsed)

class Axis(Dict):
    def __init__(self, name, children, data=None):
        super().__init__(children)
        self.name = name
        self.data = data
    
    @staticmethod
    def from_iterable(name, iterable):
        return Axis(name, dict(enumerate(iterable)))

    def copy(self):
        return Axis(self.name, Dict(self.children).copy().children, self.data)
    
    def get_coords(self, axis_name):
        if self.name == axis_name:
            return set(self.children)
        return super().get_coords(axis_name)
    

    def collapse(self, axis_name, coord):
        if self.name == axis_name:
            if coord in self.children:
                collapsed = self.children[coord].collapse(axis_name, coord)
                return collapsed
            return Undefined
        
        collapsed = {}
        for key, child in self.children.items():
            collapsed_child = child.collapse(axis_name, coord)
            if collapsed_child is Undefined:
                continue
            collapsed[key] = collapsed_child
        if len(collapsed) == 0:
            return Undefined
        return Axis(self.name, collapsed, self.data)

    def iterate(self):
        for child in self.children.values():
            for val in child.iterate():
                yield val
    
    def map(self, func, white_list=None):
        if not white_list is None and not self.name in white_list:
            return super().map(func, white_list) 
        new_children = {}
        for key, val in self.children.items():
            returned = val.map(func, white_list)
            if returned is Undefined:
                continue
            new_children[key] = returned
        return Axis(self.name, new_children, self.data)
    
    def set(self, other):
        return other.copy()
    
    def get_axis_data(self):
        axis_data = super().get_axis_data()
        axis_data[self.name] = self.data
        return axis_data
    
    def raw(self):
        return list(map((lambda child: child.raw()), self.children.values()))
    
    def __hash__(self):
        return hash(("axis", self.name, super().__hash__()))
    
    def balance(self):
        all_axes = self.get_axis_names().difference({self.name})

        children = {}

        for key, child in self.children.items():
            child_axes = child.get_axis_names()
            missing_axes = all_axes.difference(child_axes)
            for ax in missing_axes:
                child = Axis(ax, {Unindexed:child})
            children[key] = child
        
        return Axis(self.name, children, self.data)



class Leaf(Dict):
    def __init__(self, data):
        super().__init__({})
        self.data = data
    
    def copy(self):
        return Leaf(self.data)

    def raw(self):
        return self.data
    
    def collapse(self, axis_name, coord):
        return self.copy()
    
    def set(self, other):
        return other.copy()
    
    def __hash__(self):
        return hash(("leaf", hash(self.data)))

def to_tree(obj):
    if issubclass(type(obj), Dict):
        return obj
    if type(obj) is Frame:
        return obj.get_tree()
    
    if type(obj) is dict:
        return Dict({key:to_tree(val) for key,val in obj.items()})
    return Leaf(obj)

class TreePointer:
    def __init__(self, tree):
        self.tree = tree
        #self.identifier = int(time.time()*(10**15))
    

class Frame(WithOperators):
    @staticmethod
    def from_iterable(iterable):
        tree = Axis.from_iterable(0, map(to_tree, iterable))
        tp = TreePointer(tree)
        return Frame(tp)

    @staticmethod
    def make(data):
        tree = to_tree(data).transpose(normalize=True)
        return Frame(TreePointer(tree))
    
    def __init__(self, tree_pointer=None, cursor_tree = None):
        super().__init__(take_frames)
        if tree_pointer is None:
            tree_pointer = TreePointer(Dict({}))
        if cursor_tree is None:
            cursor_tree = Leaf(())
        self.tp = tree_pointer
        self.cursor = cursor_tree
        #self.identifier = hash((self.tp.identifier, hash(self.cursor)))
    
    def __getattr__(self, key):
        if key in self.__dict__: 
            return self.__dict__[key]
        if key in type(self).__dict__:
            return type(self).__dict__[key]
        return take_frames(lambda datapoint: getattr(datapoint, key))(self)

    # def add_cursor_axes(self):

    #     old_axes = self.tp.tree.get_axis_names()
        
    #     new_axes = self.cursor.get_axis_names().difference(old_axes)
    #     for axis in new_axes:
    #         self.tp.tree = self.tp.tree.map(lambda dic: Axis(axis, {Unindexed:dic}))


    def get_tree(self):
        tree = IncompleteDict({
            "data":self.tp.tree,
            "cursor":self.cursor,
        })
        
        tree = tree.transpose(normalize=True)

        def out(dic):
            if not "cursor" in dic.children or not "data" in dic.children:
                return Undefined
            output = dic.children["data"]
            for key in dic.children["cursor"].data:
                output = output.children[key]
            return output
        
        return tree.map(out)
    
    def select(self, key):
        key_tree = to_tree(key)
        tree = IncompleteDict({"cursor":self.cursor, "key":key_tree}).transpose(normalize=True)
        def update_cursor(dic):
            if not "cursor" in dic.children:
                return Undefined
            if not "key" in dic.children:
                return Undefined
            return Leaf( (*dic.children["cursor"].data, dic.children["key"].data) )
            
        new_cursor = tree.map(update_cursor)
        #self.add_cursor_axes()
        return Frame(self.tp, new_cursor)

    # def where(self, mask):
    #     mask = to_tree(mask)


    #     old_axes = self.tp.tree.get_axis_names()
    #     old_axes.update(self.cursor.get_axis_names())
    #     new_axes = mask.get_axis_names().difference(old_axes)
        
    #     tree = IncompleteDict({"cursor":self.cursor, "mask":mask}).transpose(*old_axes)
    #     def update_cursor(dic):
    #         if not "cursor" in dic.children:
    #             tree = dic.children["data"]
    #             for axis in new_axes:
    #                 tree = Axis(axis, {Unindexed:tree})
    #             return tree.transpose(normalize=True)
    #         def _update_cursor(dic):
    #             if not "mask" in dic.children:
    #                 return Undefined
    #             if dic.children["mask"].data is False:
    #                 return Undefined
    #             return dic.children["cursor"]
    #         tree = dic.transpose(normalize=True)
    #         return tree.map(_update_cursor)
    #     new_cursor = tree.map(update_cursor)
    #     #self.add_cursor_axes()
    #     return Frame(self.tp, new_cursor)


    def where(self, mask):
        mask = to_tree(mask)
        
        tree = IncompleteDict({"cursor":self.cursor, "mask":mask}).transpose(normalize=True)
        def update_cursor(dic):
            if not "mask" in dic.children:
                return Undefined
            if not "cursor" in dic.children:
                return Undefined
            if dic.children["mask"].data is False:
                return Undefined
            return dic.children["cursor"]
        new_cursor = tree.map(update_cursor)
        return Frame(self.tp, new_cursor)
    
    def drop(self):
        tree = IncompleteDict({
            "data":self.tp.tree,
            "cursor":self.cursor
        }).transpose(normalize=True)

        def update_data(dic):
            if not "cursor" in dic.children and "data" in dic.children:
                return dic.children["data"]
            return Undefined
        self.tp.tree = tree.map(update_data)

    
    def configure(self, *args, **kwargs):
        configuration = kwargs
        
        for configuration in [kwargs, *args]:
            configuration = to_tree(configuration)

            old_axes = self.tp.tree.get_axis_names()
            #old_axes.update(self.cursor.get_axis_names())
            new_axes = configuration.get_axis_names().difference(old_axes)
            
            tree_untransposed = IncompleteDict({
                "data":self.tp.tree,
                "cursor":self.cursor,
                "config":configuration
            })
            
            tree = tree_untransposed.transpose(*old_axes)

            def update_data(dic):
                if not "data" in dic.children:
                    return Undefined
                
                if not "cursor" in dic.children:
                    tree = dic.children["data"]
                    for axis in new_axes:
                        tree = Axis(axis, {Unindexed:tree})
                    return tree.transpose(normalize=True)
                
                
                config = dic.children["config"]
                for key in reversed(dic.children["cursor"].data):
                    config = Dict({key:config})
                
                tree = IncompleteDict({"data":dic.children["data"], "config":config, "cursor":dic.children["cursor"]}).transpose(normalize=True)
                return tree.map(lambda dic: dic.children["data"].set(dic.children["config"]) if "config" in dic.children else Undefined)
            
            self.tp.tree = tree.map(update_data)
        return self
    
    def __iter__(self):
        def make_datapoint(dic):
            raw = dic.raw()
            return Leaf(raw)
        for leaf in self.get_tree().map(make_datapoint).iterate():
            yield leaf.data
    
    def __getitem__(self, key):
        return self.select(key)

    def __setitem__(self, key, val):
        return self.configure({key:val})
    
    def __len__(self):
        return sum(1 for _ in self.__iter__())
    
    def map(self, func):
        tree = IncompleteDict({"func":to_tree(func), "data":self.get_tree()}).transpose(normalize=True)
        tree = tree.map(lambda dic: to_tree(dic.children["func"].data(**dic.children["data"].raw())) if "data" in dic.children and "func" in dic.children else Undefined)
        return Frame(TreePointer(tree))
    
    def __call__(self, *args, **kwargs):
        f = Frame.make({"func":self, "all_args" : _to_kwds(args, kwargs)})
        def call(dic):
            func = dic["func"]
            args, kwargs = _from_kwds(dic["all_args"])
            return func(*args, **kwargs)
        return take_frames(call)(f)
    
    
    def indexing(self, other):
        if type(other) is Frame:
            it = other.get_tree().iterate()
        else:
            it = map(Leaf, other)
        return Frame(TreePointer(self.get_tree().map(lambda dic: next(it))))

        
def vary(variations):
    tree = to_tree(variations).transpose(normalize=True)
    name = int(time.time()*(10**10))
    return tree.map(lambda dic: Axis.from_iterable(name, map(to_tree,dic.data)))

def _is_normal(obj):
    if issubclass(type(obj), Frame) or issubclass(type(obj), Dict):
        return False

    if type(obj) is dict:
        for val in obj.values():
            if not _is_normal(val):
                return False
    if type(obj) is list:
        for val in obj:
            if not _is_normal(val):
                return False
    return True

def _to_kwds(args, kwargs):
    return {
            **kwargs,
            **dict(enumerate(args))
        }

def _from_kwds(kwargs):
    args_dict = {}
    extracted_kwargs = {}
    for key, val in kwargs.items():
        if type(key) is int:
            args_dict[key] = val
        else:
            extracted_kwargs[key] = val
    
    extracted_args = [args_dict[k] for k in sorted(args_dict)]

    return extracted_args, extracted_kwargs


def take_frames(func):
    def decorated(*args, **kwargs):
        all_args = _to_kwds(args, kwargs)
        if _is_normal(all_args):
            return func(*args, **kwargs)
        f = Frame.make(all_args)
        ff = Frame()
        ff["all_args"] = f

        def extract_and_run(all_args):
            args, kwargs = _from_kwds(all_args)
            return func(*args, **kwargs)
            
        return ff.map(extract_and_run)

    return decorated

# # import numpy as np
# # f = Frame().configure({"a":vary(range(1,3)), "b":vary(range(1,4)), "c":vary(range(1,6))})
# # f["b"] = f["a"] * f["b"]
# # f["c"] = f["b"] * f["c"]
# # c = f["c"].group_by(f["a"], f["b"])
# # c_mean = c.map(np.mean)
# # c_std = c.map(np.std)
# # f["c_norm"] = (f["c"] - c_mean) / c_std
# # #f["c_norm"] = f["c_norm"]/ c_std
# # list(f)
# # list(f["b"].group_by(f["a"]).map(len))
# # list(f["b"].group_by(f["a"]).group_by(f["c"]).map(np.std))


# f = Frame()
# f["a"] = vary(range(5))
# f["b"] = vary([2,3])
# plus7 = f["a"].add(7)
# subset = f.where(f["a"].eq(3))
# subset["c"] = vary([plus7, f["b"]])


# # f = Frame()
# # f["a"] = vary([0,1])
# # f["b"] = vary([f["a"].add(10),1])
# # group_vals = f["a"].add(f["b"]).mod(2)
# # groups = f["b"].group_by(group_vals)
# # maxes = groups.map(max)
# # f["b"] = maxes.sub(f["b"])


# ff = Frame()
# ff["x"] = vary(range(3))
# ff["y"] = ff.indexing([4,7,3])




# f = Frame()
# f["a"] = vary([0,1])
# f["b"] = vary([0,1])
# print(list(f))

# f = Frame()
# f["cheese"] = 7
# print(list(f))
# f["ham"] = vary([1,2])

# f.where(f["ham"].eq(1))["cheese"] = vary([7,8])

# f = Frame()
# f["b"] = 1
# f["a"] = vary(range(5))
# subset = f.where(f["a"].mod(2).eq(0))
# subset["b"] = vary([1,2])
# list(f)

# f = Frame.make(dict(a=vary(range(5)), b = 1))
# odd = f.where(f["a"].map(lambda x: x % 2 == 1))
# odd["b"] = vary(odd["a"].map(range))
# list(f)
# f.where(f["b"].mod(2).eq(0))["c"].configure({"d":vary([1,2]), "e":vary([1,2]), "f":77})
# odd.drop()
# list(odd)


# f = Frame()
# f["a"] = vary([1,2])
# f["b"] = vary([f["a"].add(10), f["a"].add(100)])

# print("hello")




# import copy
# import string
# import random
# import operator

# ID_LEN = 10

# def rand_id():
#     state = random.getstate()
#     random.seed()
#     letters = string.ascii_letters + string.digits
#     ret = ''.join(random.choice(letters) for i in range(ID_LEN))

#     random.setstate(state)
#     return ret

# class Undefined:
#     pass
# class Unindexed:
#     pass
# class Dict:
#     def __init__(self, children):
#         self.children = children
    
#     def copy(self):
#         return Dict({key:child.copy() for key,child in self.children.items()})

#     def collapse(self, axis_name, coord):
#         collapsed = {}
#         for key, child in self.children.items():
#             collapsed_child = child.collapse(axis_name, coord)
#             if collapsed_child is Undefined:
#                 continue
#             collapsed[key] = collapsed_child
#         return Dict(collapsed)
    
#     def get_coords(self, axis_name):
#         l = [child.get_coords(axis_name) for child in self.children.values()]
#         if len(l)==0:
#             return set([])
#         return set.union(*l)
    
#     def transpose(self, *axis_names, normalize=False):
#         if normalize:
#             axis_names = (*axis_names, *self.get_axis_names().difference(set(axis_names)))
#         if len(axis_names) == 0:
#             return self.copy()
#         axis_name = axis_names[0]
#         coords = self.get_coords(axis_name)
#         if len(coords) == 0:
#             return self.collapse(axis_name, Undefined).transpose(*axis_names[1:])
#         projections = {}
#         for coord in coords:
#             proj = self.collapse(axis_name, coord).transpose(*axis_names[1:])
#             if proj is Undefined:
#                 continue
#             projections[coord] = proj
#         return Axis(axis_name, projections)
    
#     def iterate(self):
#         yield self
    
#     def raw(self):
#         return {key:child.raw() for key,child in self.children.items()}
    
#     def map(self, func):
#         return func(self)
    
#     def set(self, other):
#         if type(other) is Leaf:
#             return other
#         new = self.copy()
#         for key, child in other.children.items():
#             if key in new.children:
#                 new.children[key] = new.children[key].set(child)
#                 continue
#             new.children[key] = child
#         return new
    
#     def get_axis_names(self):
#         l = [child.get_axis_names() for child in self.children.values()]
#         if len(l) == 0:
#             return set([])
#         return set.union(*l)

#     def flatten(self, new_axis_name):
#         return Axis.from_iterable(new_axis_name, self.iterate())

# class Axis(Dict):
#     def __init__(self, name, children):
#         super().__init__(children)
#         self.name = name
    
#     @staticmethod
#     def from_iterable(name, iterable):
#         return Axis(name, dict(enumerate(iterable)))

#     def copy(self):
#         return Axis(self.name, Dict(self.children).copy().children)
    
#     def get_coords(self, axis_name):
#         if self.name == axis_name:
#             return set(self.children)
#         return super().get_coords(axis_name)
    

#     def collapse(self, axis_name, coord):
#         if self.name == axis_name:
#             if coord in self.children:
#                 return self.children[coord]
#             if coord is Undefined:
#                 return Undefined
#             return Undefined
        
#         new_children = Dict(self.children).collapse(axis_name, coord).children
#         return Axis(self.name, new_children)

#     def iterate(self):
#         for child in self.children.values():
#             for val in child.iterate():
#                 yield val
    
#     def map(self, func):
#         new_children = {}
#         for key, val in self.children.items():
#             returned = val.map(func)
#             if returned is Undefined:
#                 continue
#             new_children[key] = returned
#         return Axis(self.name, new_children)
    
#     def set(self, other):
#         return other.copy()
    
#     def get_axis_names(self):
#         return super().get_axis_names().union({self.name})
    
#     def raw(self):
#         return list(map((lambda child: child.raw()), self.children.values()))

    

# class Leaf(Dict):
#     def __init__(self, data):
#         super().__init__({})
#         self.data = data
    
#     def copy(self):
#         return Leaf(self.data)

#     def raw(self):
#         return self.data
    
#     def collapse(self, axis_name, coord):
#         return self.copy()
    
#     def set(self, other):
#         return other.copy()

# def to_tree(obj):
#     if type(obj) in [Frame,Vary]:
#         return obj.get_tree()
    
#     if type(obj) is dict:
#         return Dict({key:to_tree(val) for key,val in obj.items()})
#     return Leaf(obj)


# class TreePointer:
#     def __init__(self, tree):
#         self.tree = tree

# class Frame:
#     @staticmethod
#     def from_iterable(iterable):
#         tree = Axis.from_iterable(rand_id(), map(to_tree, iterable))
#         tp = TreePointer(tree)
#         return Frame(tp)

#     @staticmethod
#     def make(data):
#         tree = to_tree(data).transpose(normalize=True)
#         return Frame(TreePointer(tree))
    
#     def __init__(self, tree_pointer=None, cursor_tree = None):
#         if tree_pointer is None:
#             tree_pointer = TreePointer(Dict({}))
#         if cursor_tree is None:
#             cursor_tree = Leaf(())
#         self.tp = tree_pointer
#         self.cursor = cursor_tree
    
#     def get_tree(self):
#         tree = Dict({
#             "data":self.tp.tree,
#             "cursor":self.cursor,
#         })
        
#         tree = tree.transpose(normalize=True)

#         def out(dic):
#             if dic.children["cursor"].data is None or not "data" in dic.children:
#                 return Undefined
#             output = dic.children["data"]
#             for key in dic.children["cursor"].data:
#                 output = output.children[key]
#             return output
        
#         return tree.map(out)
    
#     def select(self, key):
#         key_tree = to_tree(key)
#         tree = Dict({"cursor":self.cursor, "key":key_tree}).transpose(normalize=True)
#         def update_cursor(dic):
#             if dic.children["cursor"].data is None:
#                 return Leaf(None)
#             return Leaf( (*dic.children["cursor"].data, dic.children["key"].data) )
            
#         new_cursor = tree.map(update_cursor)
#         return Frame(self.tp, new_cursor)
    
#     def where(self, mask):
#         mask = to_tree(mask)
#         tree = Dict({"cursor":self.cursor, "mask":mask}).transpose(normalize=True)
#         def update_cursor(dic):
#             if dic.children["cursor"].data is None:
#                 return Leaf(None)
#             if dic.children["mask"].data is False:
#                 return Leaf(None)
#             return dic.children["cursor"]
#         new_cursor = tree.map(update_cursor)
#         return Frame(self.tp, new_cursor)
    
#     def drop(self):
#         tree = Dict({
#             "data":self.tp.tree,
#             "cursor":self.cursor
#         }).transpose(normalize=True)

#         def update_data(dic):
#             if dic.children["cursor"].data is None and "data" in dic.children:
#                 return dic.children["data"]
#             return Undefined
#         self.tp.tree = tree.map(update_data)

    
#     def configure(self, configuration):
#         configuration = to_tree(configuration)

#         old_axes = self.tp.tree.get_axis_names()
#         new_axes = configuration.get_axis_names().difference(old_axes)
        
#         tree = Dict({
#             "data":self.tp.tree,
#             "cursor":self.cursor,
#             "config":configuration
#         }).transpose(*old_axes)

#         def update_data(dic):
            
#             if dic.children["cursor"].data is None:
#                 tree = dic.children["data"]
#                 for axis in new_axes:
#                     tree = Axis(axis, {Unindexed:tree})
#                 return tree.transpose(normalize=True)
            
#             config = dic.children["config"]
#             for key in reversed(dic.children["cursor"].data):
#                 config = Dict({key:config})
            
#             tree = Dict({"data":dic.children["data"], "config":config, "cursor":dic.children["cursor"]}).transpose(normalize=True)

#             return tree.map(lambda dic: dic.children["data"].set(dic.children["config"]))
        
#         self.tp.tree = tree.map(update_data)
#         return self
    
#     def __iter__(self):
#         def make_datapoint(dic):
#             return Leaf(dic.raw())
#         for leaf in self.get_tree().map(make_datapoint).iterate():
#             yield leaf.data
    
#     def __getitem__(self, key):
#         return self.select(key)

#     def __setitem__(self, key, val):
#         return self.configure({key:val})
    
#     def __len__(self):
#         return len(self.tp.tree.children)
    
#     def map(self, func):
#         tree = Dict({"func":to_tree(func), "data":self.get_tree()}).transpose(normalize=True)
#         tree = tree.map(lambda dic: to_tree(dic.children["func"].data(dic.children["data"].raw())))
#         return Frame(TreePointer(tree))

#     def __getattr__(self, key):
#         if key in self.__dict__:
#             return self.__dict__[key]
#         if key in operator.__dict__ and key[0]!="_":
#             op = operator.__dict__[key]
#             return self.map(lambda datapoint: lambda other: op(datapoint, other))
#         return self.map(lambda datapoint: getattr(datapoint, key))
    
#     def __call__(self, *args, **kwargs):
#         return self.map(lambda func: func(*args, **kwargs))
    
#     def group(self, other):
#         new_axis_name = rand_id()

#         def modify_data(dic):
#             data = dic.children["data"]
#             group = Unindexed
#             if "group" in dic.children:
#                 group = dic.children["group"].data
#             return Axis(new_axis_name, {group:data})
        
#         self.tp.tree = Dict({"data":self.tp.tree, "group":self.get_tree()}).transpose(normalize=True).map(modify_data)
#         if not self.tp is other.tp:
#             other.tp.tree = Dict({"data":other.tp.tree, "group":self.get_tree()}).transpose(normalize=True).map(modify_data)

        
# class Vary:
#     def __init__(self, variations):
#         tree = to_tree(variations).transpose(normalize=True)
#         name = rand_id()
#         self.tree = tree.map(lambda dic: Axis.from_iterable(name, map(to_tree,dic.data)))

#     def get_tree(self):
#         return self.tree

# def _is_normal(obj):
#     if issubclass(type(obj), Frame) or issubclass(type(obj), Vary):
#         return False

#     if type(obj) is dict:
#         for val in obj.values():
#             if not _is_normal(val):
#                 return False
#     if type(obj) is list:
#         for val in obj:
#             if not _is_normal(val):
#                 return False
#     return True

# def _to_kwds(args, kwargs):
#     return {
#             **kwargs,
#             **dict(enumerate(args))
#         }

# def _from_kwds(kwargs):
#     args_dict = {}
#     extracted_kwargs = {}
#     for key, val in kwargs.items():
#         if type(key) is int:
#             args_dict[key] = val
#         else:
#             extracted_kwargs[key] = val
    
#     extracted_args = [args_dict[k] for k in sorted(args_dict)]

#     return extracted_args, extracted_kwargs


# def take_frames(func):
#     def decorated(*args, **kwargs):
#         all_args = _to_kwds(args, kwargs)
#         if _is_normal(all_args):
#             return func(*args, **kwargs)
#         f = Frame.make(all_args)

#         def extract_and_run(all_args):
#             args, kwargs = _from_kwds(all_args)
#             return func(*args, **kwargs)
            
#         return f.map(extract_and_run)

#     return decorated

# f = Frame()
# f["a"] = Vary([0,1])
# f["b"] = Vary([0,1])
# tree = f.tp.tree.map(lambda dic:Axis("bla", {dic.children["a"].data: dic}))

# print("hello")

# # op_names = [name for name in operator.__dict__ if name[0]!=0]
# # globals().update({name:take_frames(op) for name,op in operator.__dict__.items() if name[0]!="_"})

# # f = Frame()
# # f["a"] = Vary([0,1])
# # f["b"] = Vary([0,1])
# # print(list(f))

# # f = Frame()
# # f["cheese"] = 7
# # print(list(f))
# # f["ham"] = Vary([1,2])

# # f.where(f["ham"].eq(1))["cheese"] = Vary([7,8])

# # f = Frame()
# # f["b"] = 1
# # f["a"] = Vary(range(5))
# # subset = f.where(f["a"].mod(2).eq(0))
# # subset["b"] = Vary([1,2])

# # f = Frame.make(dict(a=Vary(range(5)), b = 1))
# # odd = f.where(f["a"].map(lambda x: x % 2 == 1))
# # odd["b"] = Vary(odd["a"].map(range))
# # list(f)
# # f.where(f["b"].mod(2).eq(0))["c"].configure({"d":Vary([1,2]), "e":Vary([1,2]), "f":77})
# # odd.drop()
# # list(odd)


# # f = Frame()
# # f["a"] = Vary([1,2])
# # f["b"] = Vary([f["a"].add(10), f["a"].add(100)])

# # print("hello")

