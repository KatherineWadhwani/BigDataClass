

            topGoog = "empty"
            topMsft = "empty"

            def findHigherGoog(tenDay, fortyDay):
                        global topGoog
                        oldTop = topGoog
                        if (tenDay > fortyDay):
                                    topGoog = "tenDay"
                                    if (oldTop == topGoog):
                                                return "1"
                                    if (oldTop != topGoog and oldTop == "empty"):
                                                return "2"
                                    if (oldTop != topGoog and oldTop != "empty"):
                                                return "buy "
                        else:
                                    topGoog = "fortyDay"
                                    if (oldTop == topGoog):
                                                return "3"
                                    if (oldTop != topGoog and oldTop == "empty"):
                                                return "4"
                                    if (oldTop != topGoog and oldTop != "empty"):
                                                return "sell "


          findHigherGoog(4, 10)
          findHigherGoog(20, 10)



