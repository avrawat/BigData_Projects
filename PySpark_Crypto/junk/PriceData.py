class PriceData:

    def __init__(self,op=0,hp=0,lp=0,cp=0,vol=0):
        self.__open = float(op)
        self.__high = float(hp)
        self.__low = float(lp)
        self.__close = float(cp)
        self.__volume = abs(float(vol))
    
    def getOpen(self):
        return self.__open    
    def getClose(self):
        return self.__close    
    def getHigh(self):
        return self.__high    
    def getLow(self):
        return self.__low
    def getVolume(self):
        return self.__volume

    def setOpen(self,value):
        self.__open = float(value)    
    def setHigh(self,value):
        self.__high = float(value)        
    def setLow(self,value):
        self.__low = float(value)        
    def setClose(self,value):
        self.__close = float(value)
    def setVolume(self,value):
        self.__volume = abs(float(value))


    def __str__(self):
        return "Price[Open:"+str(self.__open)+",High:"+str(self.__high)+",Low:"+str(self.__low)+",Close:"+str(self.__close)+",Volume:"+str(self.__volume)+"]"
