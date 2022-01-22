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
        return "Symbol:"+self.__symbol+",TimeStamp:"+self.__timeStamp+","+str(self.__priceData)

