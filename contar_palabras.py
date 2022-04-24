# -*- coding: utf-8 -*-

from pyspark import SparkContext
import sys


def main(input_filename, output_filename):
    with SparkContext() as sc:
        sc.setLogLevel("ERROR")
        data = sc.textFile(input_filename)
        words_rdd = data.map(lambda x: len(x.split()))
        with open(output_filename, 'w') as outfile:
            
            outfile.write('RESULTS------------------\n')
            outfile.write('Nº de palabras: '+ str(words_rdd.sum()))
    

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python3 {0} <infilename> <outfilename>".format(sys.argv[0]))
    else:
        main(sys.argv[1], sys.argv[2])
