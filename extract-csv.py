#!/usr/bin/python
# coding: utf8

# pour les dependances :
# pip install progression
# pip install untangle
# é
import zipfile
import glob
import untangle
import csv
import sys
import progression as pr

class DeeCdataRepl:
	def __init__(self, cdata):
		self.cdata = cdata

class Dee:
	def __init__(self, xml):
		self.d = untangle.parse(xml)
	
	def fetch(self,path):
		if path == 'codesCommunes':
			return self.__codesCommunes()
		elif path == 'codesMailles':
			return self.__codesMailles()
		elif path == 'geometrie':
			return self.__geometrie()
		p = self.d
		for e in path.replace(':','_').split('/'):
			p = p.__getattr__(e)
		return p

	def __codesCommunes(self):
		try:
			communes = self.fetch("gml:featureMember/sinp:SujetObservation/sinp:communes")
		except:
			return DeeCdataRepl("")
		codes = []
		for c in communes:
			codes.append(c.sinp_Commune.sinp_codeCommune.cdata)
		return DeeCdataRepl("/".join(codes))

	def __codesMailles(self):
		mailles = self.fetch("gml:featureMember/sinp:SujetObservation/sinp:mailles")
		codes = []
		for m in mailles:
			codes.append(m.sinp_Maille10x10.sinp_codeMaille.cdata)
		return DeeCdataRepl(str.join('/',codes))
	
	def __geometrie(self):
		objgeo = self.fetch("gml:featureMember/sinp:SujetObservation/sinp:objetGeo")
		if objgeo.get_attribute('xsi:nil'):
			return DeeCdataRepl("")
		return DeeCdataRepl("WKT")


class dee_extract_csv:
	def __init__(self, cols_def, zip_list, output):
		self.zip_list = zip_list
		self.output = open(output, 'wb')
		self.csvwriter = csv.writer(self.output)
		self.cols_def = cols_def
                self.csv_encode_warn = open("%s_with_encode_probs.csv" % (output.split('.')[0]),"wb")

	def csv_row_head(self):
    		self.csvwriter.writerow(self.cols_def.keys())

	def flushzip_content(self, with_progress=False):
		self.csv_row_head()
		for zip_file in self.zip_list:
			z = zipfile.ZipFile(zip_file, 'r')
			zip_content = z.namelist()
			if with_progress:
				pn = pr.UnsignedIntValue()
				pb = pr.ProgressBar(count = pn, max_count = len(zip_content), interval=0.2)
				pb.start()
			n = 0
			errors = []
			for dee_name in zip_content:
                                if with_progress:
                                    pn.value = n
				n+=1
                                if not dee_name.endswith(".xml"):
                                    continue
				with z.open(dee_name,'r') as f:
					try:
                                          s = f.read()
					  dee = Dee(s)
					except Exception as e:
					  if with_progress:
					    pb.stop()
					  print "read %s : %s" % (dee_name, e)
				          return False
					f.close()
					l = []
					no_error = True
					for k in self.cols_def.keys():
						try:
							l.append(dee.fetch(self.cols_def[k]).cdata)
						except Exception as e:
                                                        if k == "cdRef":
                                                            l.append("")
                                                            continue
                                                        elif k == "cdNom":
                                                            l.append("")
                                                            continue
							no_error = False
							if with_progress:
								pb.stop()
							print "skip %s [%s] : %s" % (dee_name,k,e)
							raise e
							return False
					if no_error:
                                                try:
						    self.csvwriter.writerow(l)
                                                except UnicodeEncodeError as e:
                                                    # fallback
                                                    sep = ","
                                                    row = ""
                                                    for ele in l:
                                                        row = "%s%s," % (row, ele.replace(","," ").replace("\""," "))
                                                    self.csv_encode_warn.write(row.encode('utf-8')+"\n")
				f.close()
			if with_progress:
				pb.stop
			print errors
			z.close()

if __name__ == '__main__':
	sujetobs = "gml:featureMember/sinp:SujetObservation/"
	cols_def = {
		"nomCite":                      sujetobs + "sinp:nomCite",
		"statutObservation":            sujetobs + "sinp:statutObservation",
		"cdNom":                        sujetobs + "sinp:cdNom",
		"cdRef":                        sujetobs + "sinp:cdRef",
		"dateDebut":                    sujetobs + "sinp:dateDebut",
		"dateFin":                      sujetobs + "sinp:dateFin",
		"dEEDateDerniereModification":  sujetobs + "sinp:source/sinp:Source/sinp:dEEDateDerniereModification",
		"dEEDateTransformation":        sujetobs + "sinp:source/sinp:Source/sinp:dEEDateTransformation",
		"dSPublique":                   sujetobs + "sinp:source/sinp:Source/sinp:dSPublique",
		"dEEFloutage":                  sujetobs + "sinp:source/sinp:Source/sinp:dEEFloutage",
		"codesCommunes":                "codesCommunes",
		"codesMailles":                 "codesMailles",
		"geom":                         "geometrie",
		"identifiantPermanent":         sujetobs + "sinp:identifiantPermanent"
	}
	b = dee_extract_csv(cols_def, glob.glob("./*.zip"), "happy.csv")
	b.flushzip_content(with_progress=False)
