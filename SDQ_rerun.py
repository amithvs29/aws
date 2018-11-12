def itemcopy(itemList):
    with open('rerun_item_responses.bat', 'w') as f:
        f.write("set AWS_PROFILE=dmf-prod\n")
        for i in itemList:
            cmd = 'aws s3 cp s3://pine-dmf-itemresponse/{}.xml s3://pine-dmf-vendor-cos-itemresponse/{}.xml\n'.format(i, i)
            f.write(cmd)


def sdqcopy(sdqList):
    with open('rerun SDQs.txt', 'w') as f:
        f.write("set AWS_PROFILE=dmf-prod\n")
        for i in sdqList:
            cmd = 'aws s3 cp s3://pine-dmf-sdqresponse/{}.xml s3://pine-dmf-vendor-cos-sdqresponse/{}.xml\n'.format(i, i)
            f.write(cmd)

s = [68935,70648,69921,71529,71565,71288,71881,72032,69886,70267,71512,69995,71745,66668,70266,70717,71378,69972,70700,71658,70311,70104,69889,71071,70876,71476,69923,70960,69641,71377,70844,69837,70343,68571,71319,69948,69894,71109,70153,70624,70965,71315,71596,70757,71337,69810,70367,66637,71608,70999,71314,70537,70091,71527,70692,71227,70680,71400,71684,70838,71651,70912,70028,70846,70736,70111,71691,70092,69084,70674,69784,71281,70158,71372,68607,70744,70065,70093,70759,71624,69939,71317,66696,70643,71703,69892,69820,71550,71373,66628,69919,70050,70263,68770,68711,71545,71671,71627,70335,71484,70783,70604,70192,71388,66742,71631,71549,71515,71704,69817,70013,69099,70848,70966,71889,69555,70071,70441,71674,69996,73453,70215,70833,71041,70094,71503,70872,70404,71539,70132,70291,70011,70514,71045,69329,70795,71112,69282,70890,71264,66705,70827,71629,66604,72174,70714,66750,72125,69943,66617,66664,71410,71370,71169,70829,69771,69953,69962,69780,71257,70659,72183,69134,70034,69990,70801,71654,69639,71882,71701,71712,69961,71422,69816,71458,69797,70649,71618,68848,70774,71525,70948,69096,69243,71492,70484]
itemcopy(s)