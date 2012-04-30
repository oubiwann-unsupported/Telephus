java_class_map = {}


class register_java_class(type):
    """
    """
    def __new__(metacls, name, bases, attrs):
        newcls = super(register_java_class, metacls).__new__(
            metacls, name, bases, attrs)
        javaname = attrs.get('java_name', '*dummy*.%s' % name)
        java_class_map[javaname] = newcls
        java_class_map[javaname.rsplit('.', 1)[-1]] = newcls
        return newcls


class JavaMimicClass(object):
    __metaclass__ = register_java_class


class SimpleSnitch(JavaMimicClass):
    java_name = 'org.apache.cassandra.locator.SimpleSnitch'
