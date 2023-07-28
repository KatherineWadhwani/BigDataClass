topGoog = "empty"
topMsft = "empty"


def findHigherGoog(tenDay, fortyDay):
                        global topGoog
                        oldTop = topGoog
                        print(topGoog)
                        if (tenDay > fortyDay):
                                    topGoog = "tenDay"
                                    if (oldTop == topGoog):
                                                print("1")
                                    if (oldTop != topGoog and oldTop == "empty"):
                                                print("2")
                                    if (oldTop != topGoog and oldTop != "empty"):
                                                print("buy")
                        else:
                                    topGoog = "fortyDay"
                                    if (oldTop == topGoog):
                                                print("3")
                                    if (oldTop != topGoog and oldTop == "empty"):
                                                print("4")
                                    if (oldTop != topGoog and oldTop != "empty"):
                                                print("buy")
                                    



findHigherGoog(4, 10)
findHigherGoog(20, 10)



