
class PriceData():

    def __init__(self,op=0,hp=0,lp=0,cp=0,vol=0):
        # Class attributes
        self.__open = float(op)
        self.__high = float(hp)
        self.__low = float(lp)
        self.__close = float(cp)
        self.__volume = abs(float(vol))
    
    < Define Getter Methods >

    
    < Define Setter method >

    # For printing the methods
    def __str__(self):
        return "Price[Open:"+str(self.__open)+",High:"+str(self.__high)+",Low:"+str(self.__low)+",Close:"+str(self.__close)+",Volume:"+str(self.__volume)+"]"
