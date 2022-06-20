import apache_beam as beam
import csv
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToText


def parse_file(element):
  for line in csv.reader([element], quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL):
      line = [s.replace('\"', '') for s in line]
      clean_line = '","'.join(line)
      final_line = '"'+ clean_line +'"'
      return final_line



def run(region, project, bucket, temploc ):
    argv = [
           # Passed in args 
           '--region={}'.format(region),
           '--project={}'.format(project),
           '--temp_location={}'.format(temploc),
           # Constructs
           '--staging_location=gs://{}/clean_input/stg/'.format(bucket),
       # Mandatory constants
           '--job_name=cleammycsv',
           '--runner=DataflowRunner'     
          ]
    filename_in = 'gs://{}/clean_input/IN_FILE.csv'.format(bucket)
    files_output = 'gs://{}/clean_output/OUT_FILE.csv'.format(bucket)
    
    options = PipelineOptions(
    flags=argv
    )

    pipeline = beam.Pipeline(options=options)
   

    clean_csv = (pipeline 
    | 'Read input file' >> beam.io.ReadFromText(filename_in)
    | 'Parse file' >> beam.Map(parse_file)
    | 'writecsv' >> beam.io.WriteToText(files_output,num_shards=10)
   )
   
    pipeline.run()

if __name__ == '__main__':
   import argparse
   
   # Create the parser  
   parser = argparse.ArgumentParser(description='Run the CSV cleaning pipeline') 