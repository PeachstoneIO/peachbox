import peachbox
import peachbox.model
import peachbox.task

""" copyright """


class MasterDataSet(peachbox.model.MasterSchema, peachbox.task.ImportProperties):
    """ MasterDataSet is the abstract base class of the user API.
    It combines the schema and provides the task for filling the properties.
    """

    mastermodel = []
    time_fill_method = fill_default 

    def execute(self):

    def fill_properties(self, row):
        values = {}
        for entry in model:
            field = entry['field']
            values[field] = fill_methods[field](row, field) 
        return self.spark_row(**values)                            
    

    def build_model(self):
        """ build mastermodel, containing field, type and fill_method for all properties"""
        schema = mastermodel
        mastermodel = [{'field':'true_as_of_seconds','Type':'IntegerType','fill_method':time_fill_method}]
        for entry in model:
            fullentry = entry
            if not 'field' in fullentry:
                raise ValueError('Field not found in schema')
            if not 'type' in fullentry:
                raise ValueError('Type not found in schema')
            if not 'fill_method' in fullentry:
                fullentry['fill_method'] = self.fill_default
            mastermodel += fullentry

    @classmethod
    def fill_name(self, name):
        return (lambda row, field: row[name])

    def fill_default(self, row, field):
        return row['field']
