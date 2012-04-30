from twisted.internet import defer, reactor


def deferwait(s, result=None):
    def canceller(my_d):
        dcall.cancel()
    d = defer.Deferred(canceller=canceller)
    dcall = reactor.callLater(s, d.callback, result)
    return d


def addtimeout(d, waittime):
    timeouter = reactor.callLater(waittime, d.cancel)
    def canceltimeout(x):
        if timeouter.active():
            timeouter.cancel()
        return x
    d.addBoth(canceltimeout)
