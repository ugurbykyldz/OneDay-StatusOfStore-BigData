import random
from datetime import datetime , timedelta

class DataGenerator:
    
    def __init__(self):
        
        self.payment_type = ["Nakit", "Post", "Hediye Kartı"]
           
        self.product = ["Elbise", "Tişört",
                        "Gömlek", "Kot Pantolon",
                        "Kot Ceket", "Pantolon",
                        "Şort", "Bluz" ,"Ceket",
                        "Etek", "Plaj Giyim",
                        "Tesettür", "Spor Ayakkabı",
                        "Günlük Ayakkabı", "Çanta",
                        "Saat", "Takı" ]
        
        
    def dataGeneretor(self):
       data  = { "userID" : self.generatorUserID(),
                "product" : self.generatorProduct(),
                "price"  : self.generatorPrice(),
                "ptype" : self.generatorPYTPE(),
                "time" : self.generayorDate()
                }
       return data
        
        
        
    def generatorProduct(self):
       return self.product[random.randrange(0, len(self.product))]
   
    def generatorPYTPE(self):
        return self.payment_type[random.randrange(0, len(self.payment_type))]
    
    def generatorPrice(self):
        return random.randrange(25, 375)
        
        
    def generatorUserID(self):
        return random.randrange(1, 2001)
    
    
    def generayorDate(self, start = "15/8/2021 0:0:0",
                end = "15/8/2021 23:55:55"):
    
        start_d = datetime.strptime(start, "%d/%m/%Y %H:%M:%S")
        end_d = datetime.strptime(end, "%d/%m/%Y %H:%M:%S")
        
        delta = end_d - start_d
        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        random_second = random.randrange(int_delta)
        return str(start_d + timedelta(seconds=random_second))
    
    
    
    
    
    
    
    
    
    
    