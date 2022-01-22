
class stocksPrice():
    def __init__(self,symbol,timeStamp,priceData):
        # Class attributes
        self.__symbol = symbol
        self.__timeStamp = timeStamp
        self.__priceData = priceData
    
    
    < Define Getter Methods >

    
    < Define Setter method >

    
    # For printing the object 
    def __str__(self):
        return "Symbol:"+self.__symbol+",TimeStamp:"+self.__timeStamp+","+str(self.__priceData)

