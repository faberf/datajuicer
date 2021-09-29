from datajuicer.switch import Switch

class Where(Switch):

    def __init__(self, assignments):
        if not all([type(c) is bool for c in assignments]):
            raise TypeError        
        super().__init__(assignments)
    
    def true(self, frame):
        return self.case(frame, True)
    
    def false(self, frame):
        return self.case(frame, False)
    
    def join(self, true, false):
        frames_dict = {True:true, False:false}
        return super().join(frames_dict)