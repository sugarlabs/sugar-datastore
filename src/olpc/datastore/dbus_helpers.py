import utils
import dbus
import _dbus_bindings


# A dbus signal emitter factory
# this is for the case where we want dbus callable methods with
# returns to also notify others connected to the bus via a signal
class emitter(object):
    """
    >>> datastore_emitter = emitter(bus, _DS_OBJECT_PATH,_DS_DBUS_INTERFACE)
    >>> datastore.emitter(signal_name, *args, *kwargs)
    """
    def __init__(self, bus, obj_path, dbus_interface):
        self._connection = bus.get_connection()
        self.message = utils.partial(_dbus_bindings.SignalMessage, obj_path,
                                     dbus_interface)

    def __call__(self, name, *args, **kwargs):
        signature = kwargs.pop('signature', None)

        m = self.message(name)

        if signature is not None:
            m.append(signature=signature, *args)
             
        self._connection.send_message(m)
        

