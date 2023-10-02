
from metaflow import FlowSpec, Parameter, step
from data_validation import retrieve_data_validation_metrics, record_validation_results

class LinearFlow(FlowSpec):

    @step
    def start(self):
        self.my_var = 'hello world'
        self.next(self.retrieve_metrics)

    @step
    def retrieve_metrics(self):
        print('the data artifact is: %s' % self.my_var)
        self.next(self.record_results)

    @step
    def record_results(self):
        print('the data artifact is: %s' % self.my_var)
        self.next(self.end)

    @step
    def end(self):
        print('the data artifact is still: %s' % self.my_var)

if __name__ == '__main__':
    LinearFlow()
