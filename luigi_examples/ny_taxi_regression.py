import luigi
import pandas as pd
import json
import pickle
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
import seaborn as sns

from inhabitation_task import LuigiCombinator, ClsParameter, RepoMeta, InhabitationTask, TaskState, states
from cls_python import FiniteCombinatoryLogic, Subtypes

from cls_luigi_read_tabular_data import WriteSetupJson, ReadTabularData
from cls_python.debug_util import deep_str


class WriteCSVRegressionSetupJson(WriteSetupJson):
    abstract = False

    def run(self):
        d = {
            "csv_file": "data/taxy_trips_ny_2016-06-01to03_3%sample.csv",
            "date_column": ['pickup_datetime', 'dropoff_datetime'],
            "target_column": 'trip_duration'
        }
        with open('data/setup.json', 'w') as f:
            json.dump(d, f)


class ReadTaxiData(ReadTabularData):
    abstract = False

    def run(self):
        setup = self._read_setup()
        taxi = pd.read_csv(setup["csv_file"], parse_dates=setup["date_column"])
        taxi.to_pickle('data/tabular_data.pkl')


class PreprocessTabularData(luigi.Task, LuigiCombinator):
    abstract = True
    tabular_data = ClsParameter(tpe=ReadTabularData.return_type())

    def requires(self):
        return self.tabular_data()

    def _read_tabular_data(self):
        return pd.read_pickle(self.input().open().name)


class PreprocessDateFeature1(PreprocessTabularData):
    abstract = False
    tabular_data = ClsParameter(tpe=ReadTabularData.return_type())

    def run(self):
        tabular = self._read_tabular_data()

        for c in tabular.columns:
            if pd.api.types.is_datetime64_any_dtype(tabular[c]):
                print("Datetime Column:", c)
                tabular[c + "_YEAR"] = tabular[c].dt.year
                tabular[c + "_MONTH"] = tabular[c].dt.hour
                tabular[c + "_DAY"] = tabular[c].dt.day


        print(tabular.columns)
        corr = tabular.corr().abs()

        # plt.figure(figsize=(10, 10))
        # sns.heatmap(corr, annot=True, cbar=False, fmt='.2f')
        # plt.tight_layout()
        # plt.savefig("data/corr_heatmap.png")
        tabular.to_pickle('data/tabular_data_preprocessed1.pkl')

    def output(self):
        return luigi.LocalTarget('data/tabular_data_preprocessed1.pkl')


class PreprocessDateFeature2(PreprocessTabularData):
    abstract = False
    tabular_data = ClsParameter(tpe=ReadTabularData.return_type())

    def run(self):
        tabular = self._read_tabular_data()

        for c in tabular.columns:
            if pd.api.types.is_datetime64_any_dtype(tabular[c]):
                print("Datetime Column:", c)
                tabular[c + "_WEEKDAY"] = tabular[c].dt.dayofweek
                tabular[c + "_HOUR"] = tabular[c].dt.hour

        tabular.to_pickle('data/tabular_data_preprocessed2.pkl')

    def output(self):
        return luigi.LocalTarget('data/tabular_data_preprocessed2.pkl')


class PreprocessorDummy(PreprocessDateFeature1, PreprocessDateFeature2):
    abstract = False
    tabular_data = ClsParameter(tpe=ReadTabularData.return_type())

    def run(self):
        tabular = self._read_tabular_data()
        tabular.to_pickle('data/tabular_data_preprocessed_dummy.pkl')

    def output(self):
        return luigi.LocalTarget('data/tabular_data_preprocessed_dummy.pkl')


d = {PreprocessDateFeature1, PreprocessDateFeature2}

class PreprocessorSet(luigi.Task, LuigiCombinator):
    date_feature1 = ClsParameter(tpe=PreprocessDateFeature1.return_type() \
        if PreprocessDateFeature1 in d else PreprocessorDummy.return_type())

    date_feature2 = ClsParameter(tpe=PreprocessDateFeature2.return_type() \
        if PreprocessDateFeature2 in d else PreprocessorDummy.return_type())

    def requires(self):
        return [self.date_feature1(), self.date_feature2()]

    def run(self):
        df = None
        for r in self.input():
            if df is not None:
                data = pd.read_pickle(r.open().name)
                df = pd.merge(df, data)
            else:
                df = pd.read_pickle(r.open().name)

        df.to_pickle('data/tabular_data_preprocessed.pkl')

    def output(self):
        return luigi.LocalTarget('data/tabular_data_preprocessed.pkl')


class TrainRegressionModel(luigi.Task, LuigiCombinator):
    abstract = True
    tabular_data_preprocessed = ClsParameter(tpe=PreprocessorSet.return_type())
    setup = ClsParameter(tpe=WriteSetupJson.return_type())

    def requires(self):
        return [self.setup(), self.tabular_data_preprocessed()]

    def output(self):
        return luigi.LocalTarget('data/regression_model.pkl')

    def _read_setup(self):
        with open(self.input()[0].open().name) as file:
            setup = json.load(file)
        return setup

    def _read_tabular_data(self):
        return pd.read_pickle(self.input()[1].open().name)


class TrainLinearRegressionModel(TrainRegressionModel):
    abstract = False

    def run(self):
        setup = self._read_setup()
        tabular = self._read_tabular_data()
        print("TARGET:", setup["target_column"])
        print("NOW WE FIT A REGRESSION MODEL")

        # Todo: Here I'm filtering to the non categorical columns!
        X = tabular[['trip_distance','trip_duration',
                    'haversine_distance', 'totalleft',
                    'totalsteps', 'totalturn', 'main_street_ratio',
                    'osrm_duration']]
        y = tabular[[setup["target_column"]]].values.ravel()
        print(y)
        print(X.shape)
        print(y.shape)
        reg = LinearRegression().fit(X, y)

        print(reg.coef_)

        with open('data/regression_model.pkl', 'wb') as f:
            pickle.dump(reg, f)


class FinalNode(luigi.WrapperTask, LuigiCombinator):
    setup = ClsParameter(tpe=WriteSetupJson.return_type())
    train = ClsParameter(tpe=TrainLinearRegressionModel.return_type())

    def requires(self):
        return [self.setup(), self.train()]


if __name__ == '__main__':
    target = FinalNode.return_type()
    print("Collecting Repo")
    repository = RepoMeta.repository
    print("Build Repository...")
    print(deep_str(repository))
    print(deep_str(RepoMeta.subtypes))
    fcl = FiniteCombinatoryLogic(repository, Subtypes(RepoMeta.subtypes), processes=1)
    print("Build Tree Grammar and inhabit Pipelines...")

    inhabitation_result = fcl.inhabit(target)
    print("Enumerating results...")
    print(deep_str(inhabitation_result.rules))
    max_tasks_when_infinite = 10
    actual = inhabitation_result.size()
    max_results = max_tasks_when_infinite
    if not actual is None or actual == 0:
        max_results = actual
    print(inhabitation_result.raw[0])
    results = [t() for t in inhabitation_result.evaluated[0:max_results]]
    if results:
        print("Number of results", max_results)
        print("Run Pipelines")
        luigi.build(results, local_scheduler=True)  # für luigid: local_scheduler = True weglassen!
    else:
        print("No results!")

    # task = InhabitationTask()
    # states[task.task_id] = TaskState(fcl, target)
    # luigi.build([task], worker_scheduler_factory=states[task.task_id].worker_scheduler_factory)