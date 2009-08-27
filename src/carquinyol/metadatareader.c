#include "Python.h"

#include <dirent.h>

// TODO: put it in a place where python can use it when writing metadata
#define MAX_PROPERTY_LENGTH 500 * 1024

static PyObject *byte_array_type = NULL;

int
add_property(const char *metadata_path, char *property_name, PyObject *dict,
             int must_exist)
{
    int file_path_size;
    char *file_path = NULL;
    FILE *file = NULL;
    long file_size;
	char *value_buf = NULL;
    PyObject *value = NULL;
    struct stat file_stat;

    // Build path of the property file
    file_path_size = strlen(metadata_path) + 1 + strlen(property_name) + 1;
    file_path = PyMem_Malloc(file_path_size);
	if (file_path == NULL) {
        PyErr_NoMemory();
        return 0;
	}
    snprintf (file_path, file_path_size, "%s/%s", metadata_path, property_name);

    if ((!must_exist) && (stat(file_path, &file_stat) != 0)) {
        PyMem_Free(file_path);
        return;
    }

    file = fopen(file_path, "r");
    if (file == NULL) {
	    char buf[256];
	    snprintf(buf, sizeof(buf), "Cannot open property file %s: %s",
	             file_path, strerror(errno));
        PyErr_SetString(PyExc_IOError, buf);
        goto cleanup;
    }

    // Get file size
    fseek (file, 0, SEEK_END);
    file_size = ftell (file);
    rewind (file);

    if (file_size == 0) {
        // Empty property
        fclose(file);
        file = NULL;

        value = PyString_FromString("");
        if (value == NULL) {
            PyErr_SetString(PyExc_ValueError,
                            "Failed to convert value to python string");
            goto cleanup;
        }
    } else {
        if (file_size > MAX_PROPERTY_LENGTH) {
            PyErr_SetString(PyExc_ValueError, "Property file too big");
            goto cleanup;
        }

        // Read the whole file
    	value_buf = PyMem_Malloc(file_size);
    	if (value_buf == NULL) {
            PyErr_NoMemory();
            goto cleanup;
    	}
        long read_size = fread(value_buf, 1, file_size, file);
        if (read_size < file_size) {
    	    char buf[256];
    	    snprintf(buf, sizeof(buf),
    	             "Error while reading property file %s", file_path);
            PyErr_SetString(PyExc_IOError, buf);
            goto cleanup;
        }

        fclose(file);
        file = NULL;

        // Convert value to dbus.ByteArray
        PyObject *args = Py_BuildValue("(s#)", value_buf, file_size);

        PyMem_Free(value_buf);
        value_buf = NULL;

        value = PyObject_CallObject(byte_array_type, args);
        Py_DECREF(args);

        if (value == NULL) {
            PyErr_SetString(PyExc_ValueError,
                            "Failed to convert value to dbus.ByteArray");
            goto cleanup;
        }
    }

    // Add property to the metadata dict
    if (PyDict_SetItemString(dict, property_name, value) == -1) {
        PyErr_SetString(PyExc_ValueError,
                        "Failed to add property to dictionary");
        goto cleanup;
    }

    Py_DECREF(value);
    PyMem_Free(file_path);

    return 1;

cleanup:
    if (file_path) {
        PyMem_Free(file_path);
    }
    if (value_buf) {
        PyMem_Free(value_buf);
    }
    if (file) {
        fclose(file);
    }
    if (value) {
        Py_DECREF(value);
    }
    return 0;    
}

static PyObject *
read_from_properties_list (const char *metadata_path, PyObject *properties)
{
    PyObject *dict = PyDict_New();

    int i;
    for (i = 0; i < PyList_Size(properties); i++) {
        PyObject *property = PyList_GetItem(properties, i);
        char *property_name = PyString_AsString (property);

        if (add_property(metadata_path, property_name, dict, 0) == 0)
            goto cleanup;
    }

    return dict;

cleanup:
    if (dict) {
        Py_DECREF(dict);
    }
    return NULL;        
}

static PyObject *
read_all_properties (const char *metadata_path)
{
    PyObject *dict = PyDict_New();
	DIR *dir_stream = NULL;
	struct dirent *dir_entry = NULL;

    dir_stream = opendir (metadata_path);
	if (dir_stream == NULL) {
	    char buf[256];
	    snprintf(buf, sizeof(buf), "Couldn't open metadata directory %s",
	             metadata_path);
        PyErr_SetString(PyExc_IOError, buf);
        goto cleanup;
	}

	dir_entry = readdir(dir_stream);
    while (dir_entry != NULL) {
        // Skip . and ..
        if (dir_entry->d_name[0] == '.' &&
            (strlen(dir_entry->d_name) == 1 || 
                (dir_entry->d_name[1] == '.' &&
                    strlen(dir_entry->d_name) == 2)))
			goto next_property;

        if (add_property(metadata_path, dir_entry->d_name, dict, 1) == 0)
            goto cleanup;

    next_property:
    	dir_entry = readdir(dir_stream);
    }

	closedir(dir_stream);

    return dict;

cleanup:
    if (dict) {
        Py_DECREF(dict);
    }
    if (dir_stream) {
    	closedir(dir_stream);
	}
    return NULL;        
}

static PyObject *
metadatareader_retrieve(PyObject *unused, PyObject *args)
{
    PyObject *dict = NULL;
    PyObject *properties = NULL;
    const char *metadata_path = NULL;

    if (!PyArg_ParseTuple(args, "sO:retrieve", &metadata_path, &properties))
        return NULL;

    if ((properties != Py_None) && (PyList_Size(properties) > 0)) {
        dict = read_from_properties_list(metadata_path, properties);
    } else {
        dict = read_all_properties(metadata_path);
    }

    return dict;
}

static PyMethodDef metadatareader_functions[] = {
    {"retrieve", metadatareader_retrieve, METH_VARARGS,
        PyDoc_STR("Read a dictionary from a directory with a single file " \
	      "(containing the content) per key")},
    {NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC
initmetadatareader(void)
{
  PyObject* mod;
  mod = Py_InitModule("metadatareader", metadatareader_functions);
  
  PyObject *dbus_module = PyImport_ImportModule("dbus");
  byte_array_type = PyObject_GetAttrString(dbus_module, "ByteArray");
}

