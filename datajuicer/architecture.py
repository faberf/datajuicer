import datajuicer.utils

# class Architecture:
#     @staticmethod
#     def default_hyperparameters():
#         pass
    
#     @staticmethod
#     def environment_parameters(mode):
#         pass
    
#     @staticmethod
#     def launch_settings(mode):
#         return {
#             "launch":"python {code_file} {make_args}"
#         }
    
#     @staticmethod
#     def checker(model):
#         pass
    
#     @classmethod
#     def make_args(cls,model):
#             keys = list({**cls.default_hyperparameters(), **cls.environment_parameters(format_template(model,"{mode}"))}.keys())
#             return " ".join([" -" + key + "=" + str(model[key])+" " for key in keys]) + " -session_id="+str(model[cls.__name__+"_session_id"])+" "

#     @classmethod
#     def system_call(cls,model):
#         model.update(cls.launch_settings(format_template(model,"{mode}")))
#         model.update(cls.environment_parameters(format_template(model,"{mode}")))
#         launch = format_template(model,"{launch}")
#         os.system(launch)


#     @classmethod
#     def make(cls):
#         return {
#             "architecture": cls.__name__,
#             "args":cls.make_args,
#             cls.__name__ + "_checker": cls.checker,
#             cls.__name__+"_function": cls.system_call,
#             cls.__name__+"_dependencies": list(cls.default_hyperparameters().keys()),
#             **cls.default_hyperparameters()
#             }

#     @staticmethod
#     def help():
#         return {}

#     @classmethod
#     def get_flags(cls, mode):
#         help=cls.help()
#         parser = argparse.ArgumentParser()
#         for key, value in {**cls.default_hyperparameters(), **cls.environment_parameters(mode)}.items():
#             parser.add_argument("-" + key,type=type(value),default=value,help=help.get(key,""))
#         parser.add_argument("-session_id", type=int, default = 0)
        
#         flags = parser.parse_args()
#         if flags.session_id==0:
#             flags.session_id = random.randint(1000000000, 9999999999)
#         return flags

#     @staticmethod
#     def log(session_id, key, value, save_dir = None):
#         if save_dir is None:
#             save_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),"Resources/TrainingResults/")
#         file = os.path.join(save_dir, str(session_id) + ".train")
#         exists = os.path.isfile(file)
#         directory = os.path.dirname(file)
#         if not os.path.isdir(directory):
#             os.makedirs(directory)
#         if exists:
#             data = open(file).read()
#             try:
#                 d = json.loads(data)
#             except:
#                 d = {}
#         else:
#             d = {}
#         with open(file,'w+') as f:
#             if key in d:
#                 d[key] += [value]
#             else:
#                 d[key]=[value]
#             json.dump(d,f)
