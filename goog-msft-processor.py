topGoog = "empty"
topMsft = "empty"


def findHigherGoog(tenDay, fortyDay):
                        global topGoog
                        oldTop = topGoog
                        print(topGoog)
                        if (tenDay > fortyDay):
                                    topGoog = "tenDay"
                                  
                        else:
                                    topGoog = "fortyDay"
                                    


findHigherGoog(4, 10)
findHigherGoog(20, 10)



