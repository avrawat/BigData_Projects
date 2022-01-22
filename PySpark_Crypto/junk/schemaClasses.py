class stocksPrice:
    def __init__(self,symbol,timeStamp,priceData):
        self.__symbol = symbol
        self.__timeStamp = timeStamp
        self.__priceData = priceData
        
    def getSymbol(self):
        return self.__symbol
    def getTimeStamp(self):
        return self.__timeStamp
    def getPriceData(self):
        return self.__priceData
    
    def setSymbol(self,value):
        self.__symbol = value
    def setTimeStamp(self,value):
        self.__timeStamp = value
    def setPriceData(self,value):
        self.__priceData = value
    
    def __str__(self):
        return "Symbol-"+self.__symbol+",TimeStamp-"+self.__timeStamp+","+str(self.__priceData)

class PriceData:
    
    def __init__(self,op,hp,lp,cp):
        self.__open = op
        self.__high = hp
        self.__low = lp
        self.__close = cp
    
    def getOpen(self):
        return self.__open    
    def getClose(self):
        return self.__close    
    def getHigh(self):
        return self.__high    
    def getLow(self):
        return self.__low
    
    def setOpen(self,value):
        self.__open = value    
    def setHigh(self,value):
        self.__high = value    
    def setLow(self,value):
        self.__low = value    
    def setClose(self,value):
        self.__close = value
    
    def __str__(self):
        return "Price[Open-"+str(self.__open)+",High-"+str(self.__high)+",Low-"+str(self.__low)+",Close-"+str(self.__close)+"]"
