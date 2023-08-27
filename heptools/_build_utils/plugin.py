def dask_sizeof_plugin(sizeof):
    @sizeof.register_lazy('awkward')
    def register_awkward():
        from awkward import Array
        @sizeof.register(Array)
        def sizeof_awkward_array(x: Array):
            return x.nbytes

    @sizeof.register_lazy('uproot')
    def register_uproot():
        from uproot import Model
        @sizeof.register(Model)
        def sizeof_uproot_model(x: Model):
            return x.num_bytes

    @sizeof.register_lazy('hist')
    def register_hist():
        from hist import Hist
        @sizeof.register(Hist)
        def sizeof_hist(x):
            return sizeof(x.view(flow = True))

    #TODO heptools