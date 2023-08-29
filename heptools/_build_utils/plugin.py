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
        def sizeof_hist(x: Hist):
            return sizeof(x.view(flow = True))

    @sizeof.register_lazy('heptools')
    def register_heptools():
        from ..container import PartialSet
        @sizeof.register(PartialSet)
        def sizeof_partialset(x: PartialSet):
            return sizeof(x._in)

        from ..root.selection import Selection
        @sizeof.register(Selection)
        def sizeof_selection(x: Selection):
            return sizeof(x.filters)