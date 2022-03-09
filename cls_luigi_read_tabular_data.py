import luigi
from inhabitation_task import LuigiCombinator, ClsParameter
import json


class WriteSetupJson(luigi.Task, LuigiCombinator):
    abstract = True

    def output(self):
        return luigi.LocalTarget('data/setup.json')


class ReadTabularData(luigi.Task, LuigiCombinator):
    abstract = True
    setup = ClsParameter(tpe=WriteSetupJson.return_type())

    def requires(self):
        return [self.setup()]

    def output(self):
        return luigi.LocalTarget('data/tabular_data.pkl')

    def _read_setup(self):
        with open(self.input()[0].open().name) as file:
            setup = json.load(file)
        return setup