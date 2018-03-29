import sys, gzip

inFilePath = sys.argv[1]
outFilePath = sys.argv[2]
inFile = open(inFilePath,"r")
inFile.readline()

sampleDict = {}
variableSet=set()
for line in inFile:
	row = line.rstrip("\n").split("\t")
	sampleID = row[0]
	variable = row[1]
	value = row[2]
	variableSet.add(variable)
	if not sampleID in sampleDict:
		sampleDict[sampleID]={}
	sampleDict[sampleID][variable]=value

outFile = open(outFilePath, "w")
variableList = list(variableSet)
headerText= "Sample\t"
for variable in variableList:
	headerText = headerText + variable + "\t"
headerText+="\n"
outFile.write(headerText)
for key in sampleDict:
	outText=key + "\t"
	for variable in variableList:
		if variable in sampleDict[key]:
			outText += sampleDict[key][variable]+"\t"
		else:

	#If the value isn't there, we will either put NA or -666 depending on the datatype
			#vType = 0
			#for value in sampleDict:
			#	if variable in sampleDict[value]:
			#		vType = sampleDict[value][variable]
			#		break
			#try:
			#	float_value = float(vType)
			#	outText+=str(-666)+"\t"
			#except ValueError:
			outText+= "NA\t"
			
	outText+="\n"
	outFile.write(outText)

	
	
