topGoog = "empty"
topMsft = "empty"


def findHigherGoog(tenDay, fortyDay):
                        global topGoog
                        oldTop = topGoog
                        print(topGoog)
                        if (tenDay > fortyDay):
                                    topGoog = "tenDay"
                                    if (oldTop == topGoog):
                                                return "1"
                                    if (oldTop != topGoog and oldTop == "empty"):
                                                print(topGoog)
                                    if (oldTop != topGoog and oldTop != "empty"):
                                                print("buy")
                        else:
                                    topGoog = "fortyDay"
                                    if (oldTop == topGoog):
                                                return "3"
                                    if (oldTop != topGoog and oldTop == "empty"):
                                                return "4"
                                    if (oldTop != topGoog and oldTop != "empty"):
                                                print("buy")
                                    



findHigherGoog(4, 10)
findHigherGoog(20, 10)



