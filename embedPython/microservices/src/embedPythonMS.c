/* Copyright (c) 2009, University of Liverpool
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *
 * Author       : Jerome Fuselier
 * Creation     : Mar 12, 2009
 */

#include <Python.h>
#include <cstdlib>
#include <unistd.h>


//#include "rsApiHandler.h"
#include "embedPythonMS.h"

#include "PyMsParam.h"

#define BIG_STR 2000


char* getFilename(char* path) {
	char str[strlen(path)];
	char *pch;
	char *filename = (char*)malloc(sizeof(char)*strlen(path));

	strcpy(str, path);

	pch = strtok (str,"/");
	while (pch != NULL)
	{
		strcpy(filename, pch);
		pch = strtok (NULL, "/");
	}

	return strtok (filename, ".");
}

char* getBasename(char* path) {
	char str[strlen(path)];
	char * pch;
	char *basename = (char*)malloc(sizeof(char)*strlen(path));

	strcpy(str, path);
	strcpy(basename, "/");

	pch = strtok (str,"/");
	while (pch != NULL)
	{
		char elem[50];
		strcpy(elem, pch);
		pch = strtok (NULL, "/");
		// For the last element
		if (pch != NULL) {
			strcat(basename, elem);
			strcat(basename, "/");
		}
	}
	return basename;
}


int readFile(rsComm_t *rsComm, char* inPath, bytesBuf_t *data)
{
	/* ********** *
	 * Initialize *
	 * ********** */

	dataObjInp_t openParam;
	openedDataObjInp_t closeParam;
	openedDataObjInp_t readParam;
	openedDataObjInp_t seekParam;
	fileLseekOut_t* seekResult = NULL;
	int fd = -1;
	int fileWasOpened = FALSE;
	int fileLength = 0;
	int status;

	memset(&openParam,  0, sizeof(dataObjInp_t));
	memset(&seekParam,  0, sizeof(openedDataObjInp_t));
	memset(&readParam,  0, sizeof(openedDataObjInp_t));
	memset(&closeParam, 0, sizeof(openedDataObjInp_t));
	memset(data,        0, sizeof(bytesBuf_t));


	/* ************* *
	 * Open the file *
	 * ************* */

	// Set the parameters for the open call
	strcpy(openParam.objPath, inPath);
	openParam.openFlags = O_RDONLY;

	status = rsDataObjOpen(rsComm, &openParam);

	if (status < 0) { return status; }
	fd = status;
	fileWasOpened = TRUE;


	/* ************************ *
     * Looking for the filesize *
	 * ************************ */

	// Go to the end of the file
	seekParam.l1descInx = fd;
	seekParam.offset  = 0;
	seekParam.whence  = SEEK_END;

	status = rsDataObjLseek(rsComm, &seekParam, &seekResult);

	if (status < 0) {
		// Try to close the file we opened, ignoring errors
		if (fileWasOpened) {
			closeParam.l1descInx = fd;
			rsDataObjClose(rsComm, &closeParam);
		}
		return status;
	}
	fileLength = seekResult->offset;

	// Reset to the start for the read
	seekParam.offset  = 0;
	seekParam.whence  = SEEK_SET;

	status = rsDataObjLseek(rsComm, &seekParam, &seekResult);

	if (status < 0) {
		// Try to close the file we opened, ignoring errors
		if (fileWasOpened) {
			closeParam.l1descInx = fd;
			rsDataObjClose(rsComm, &closeParam);
		}
		return status;
	}


	/* ************* *
	 * Read the file *
	 * ************* */


	// Set the parameters for the open call
	readParam.l1descInx = fd;
	readParam.len       = fileLength;
	data->len           = fileLength;
	data->buf           = (void*)malloc(fileLength);

	// Read the file
	status = rsDataObjRead(rsComm, &readParam, data);
	if (status < 0)	{
		free((char*)data->buf);
		// Try to close the file we opened, ignoring errors
		if (fileWasOpened) {
			closeParam.l1descInx = fd;
			rsDataObjClose(rsComm, &closeParam);
		}
		return status;
	}

	/* ************** *
	 * Close the file *
	 * ************** */

	// Close the file we opened
	if (fileWasOpened) {
		closeParam.l1descInx = fd;

		status = rsDataObjClose(rsComm, &closeParam);
		if (status < 0) {
			free((char*)data->buf);
			return status;
		}
	}

	return 0;
}


int add_microservice_in_history(char* rule_name, char* func_name,
								PyRuleExecInfo_t *pyRuleExecInfo) {
	if ( (strcmp(rule_name, "noRecursionTest") == 0) ||
			(strcmp(rule_name, "noTestRecursion") == 0) )
		return 0;

	PyObject *mainModule, *mainDict, *call_hist;
	PyObject *ruleNameDict, *func_name_list;

	mainModule = PyImport_AddModule("__main__");
	mainDict = PyModule_GetDict(mainModule);
	call_hist = PyDict_GetItemString(mainDict, "call_history");

	// This should never happened as call_historty is created in initialize
	if ( !call_hist ) {
		call_hist = PyDict_New();
		PyDict_SetItemString(mainDict, "call_history", call_hist);
	}

	// If the rule is not present yet, we create a dictionary for the rule
	if ( !PyDict_Contains(call_hist, PyString_FromString(rule_name)) ) {
		PyObject *new_dict = PyDict_New();
		PyDict_SetItemString(call_hist, rule_name, new_dict);
	}
	// Get a pointer to the dict of the rule (it stores methods/list of rei)
	ruleNameDict = PyDict_GetItemString(call_hist, rule_name);

	// If the method is not present, we create a new list with the rei
	if ( !PyDict_Contains(ruleNameDict, PyString_FromString(func_name)) ) {
		PyObject *new_list = PyList_New(0);
		PyList_Append(new_list, (PyObject *)pyRuleExecInfo);
		PyDict_SetItemString(ruleNameDict, func_name, new_list);
	} else {
		// If the method is present, we check if there's already the rei in the
		// associated list. If it is already there this may indicate a recursion
		// so we don't execute the microservice another time and return -1

		func_name_list = PyDict_GetItemString(ruleNameDict, func_name);
		int cpt = 0, size = PyList_Size(func_name_list);
		int found = 0;

		while ( (!found) && (cpt < size) ) {
			PyRuleExecInfo_t *tmpRei = (PyRuleExecInfo_t *)PyList_GetItem(func_name_list, cpt);

			found = ( cmpRuleExecInfo_t(tmpRei->ruleExecInfoPtr,
										pyRuleExecInfo->ruleExecInfoPtr) == 0 );
			cpt += 1;

		}

		if (found)
			return -1;
		else
			PyList_Append(func_name_list, (PyObject *) pyRuleExecInfo);	}

	return 0;
}


int msiRodsPython(msParam_t *irods_script_path, msParam_t *func_name, msParam_t *rule_name,
				  ruleExecInfo_t *rei)
{
	RE_TEST_MACRO( "    Calling msiRodsPython");

	char *str_script_path = parseMspForStr(irods_script_path);
	char *str_func_name = parseMspForStr(func_name);
	char *str_rule_name = parseMspForStr(rule_name);
	char long_func_name[BIG_STR];
	bytesBuf_t inData;
	int status;
	rsComm_t *rsComm = rei->rsComm;
	PyObject *pModule, *pDict, *pFunc;
	int err_code = 0;

	// Create the ruleExecInfo_t variable to be used in python
	PyRuleExecInfo_t *pyRuleExecInfo = PyObject_NEW(PyRuleExecInfo_t,
													&PyRuleExecInfo_Type);
	pyRuleExecInfo->ruleExecInfoPtr = rei;

	// Code for recursion problem

	snprintf(long_func_name, BIG_STR, "%s()", str_func_name);
	status = add_microservice_in_history(str_rule_name, long_func_name, pyRuleExecInfo);

	// If there is a recursion, status should be -1. This means that there's already
	// a call for the same rule, the same method, the same parameters and the
	// same rei
	if (status >= 0) {

		// Read the script from iRODS, get a string
		status = readFile(rsComm, str_script_path, &inData);
		if (status < 0) {
			rodsLogAndErrorMsg(LOG_ERROR, &rsComm->rError, status,
							   "%s:  could not read file, status = %d",
							   str_script_path, status);
			return USER_FILE_DOES_NOT_EXIST;
		}
		// Execute the script (It will load the functions defined in the script in
		// the global dictionary)
		char tmpStr[inData.len];
		snprintf(tmpStr, inData.len, "%s", (char *)inData.buf);
		err_code = PyRun_SimpleString(tmpStr);
		if (err_code == -1) {
			PyErr_Print();
			err_code = INVALID_OBJECT_TYPE;
			rei->status = err_code;
			return err_code;
		}
		// Get a reference to the main module and global dictionary
		pModule = PyImport_AddModule("__main__");
		pDict = PyModule_GetDict(pModule);
		// Get a reference to the function we want to call
		pFunc = PyDict_GetItemString(pDict, str_func_name);
		if (!pFunc) {
			PyErr_Print();
			err_code = NO_MICROSERVICE_FOUND_ERR;
			rei->status = err_code;
			return err_code;
		}

		// Call the python microservice with the parameter(s) and the rei
		PyObject *error_code = PyObject_CallFunction(pFunc, "O", pyRuleExecInfo);

		// If the user returns nothing, we assume there are no errors
		if (error_code == Py_None) {
			err_code = 0;
		} else {
			if (error_code)
				err_code = PyInt_AsLong(error_code);
			else {
				// if CallFunction fails error_code is NULL. This is an error in the
				// python script (wrong name, syntax error, ...
				// The PyErr_Print will print it in rodsLog
				PyErr_Print();
				err_code = INVALID_OBJECT_TYPE; // not the best one but it exists
			}
		}
	}

    rei->status = err_code;
    return err_code;
}


int msiRodsPython1(msParam_t *irods_script_path, msParam_t *func_name, msParam_t *rule_name,
		           msParam_t *param1, ruleExecInfo_t *rei)
{
	RE_TEST_MACRO( "    Calling msiRodsPython1");

	char *str_script_path = parseMspForStr(irods_script_path);
	char *str_func_name = parseMspForStr(func_name);
	char *str_rule_name = parseMspForStr(rule_name);
	char long_func_name[BIG_STR];
	bytesBuf_t inData;
	int status;
	rsComm_t *rsComm = rei->rsComm;
	PyObject *pModule, *pDict, *pFunc;
	int err_code = 0;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam1 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam1->msParamPtr = param1;

	// Create the ruleExecInfo_t variable to be used in python
	PyRuleExecInfo_t *pyRuleExecInfo = PyObject_NEW(PyRuleExecInfo_t,
													&PyRuleExecInfo_Type);
	pyRuleExecInfo->ruleExecInfoPtr = rei;

	// Code for recursion problem

	snprintf(long_func_name, BIG_STR, "%s(%s)", str_func_name, parseMspForStr(param1));
	status = add_microservice_in_history(str_rule_name, long_func_name, pyRuleExecInfo);

	// If there is a recursion, status should be -1. This means that there's already
	// a call for the same rule, the same method, the same parameters and the
	// same rei
	if (status >= 0) {

		// Read the script from iRODS, get a string
		status = readFile(rsComm, str_script_path, &inData);
		if (status < 0) {
			rodsLogAndErrorMsg(LOG_ERROR, &rsComm->rError, status,
							   "%s:  could not read file, status = %d",
							   str_script_path, status);
			return USER_FILE_DOES_NOT_EXIST;
		}
		// Execute the script (It will load the functions defined in the script in
		// the global dictionary)
		char tmpStr[inData.len];
		snprintf(tmpStr, inData.len, "%s", (char *)inData.buf);
		err_code = PyRun_SimpleString(tmpStr);
		if (err_code == -1) {
			PyErr_Print();
			err_code = INVALID_OBJECT_TYPE;
			rei->status = err_code;
			return err_code;
		}
		// Get a reference to the main module and global dictionary
		pModule = PyImport_AddModule("__main__");
		pDict = PyModule_GetDict(pModule);
		// Get a reference to the function we want to call
		pFunc = PyDict_GetItemString(pDict, str_func_name);
		if (!pFunc) {
			PyErr_Print();
			err_code = NO_MICROSERVICE_FOUND_ERR;
			rei->status = err_code;
			return err_code;
		}

		// Call the python microservice with the parameter(s) and the rei
		PyObject *error_code = PyObject_CallFunction(pFunc, "OO", pyMsParam1,
				pyRuleExecInfo);

		// If the user returns nothing, we assume there are no errors
		if (error_code == Py_None) {
			err_code = 0;
		} else {
			if (error_code)
				err_code = PyInt_AsLong(error_code);
			else {
				// if CallFunction fails error_code is NULL. This is an error in the
				// python script (wrong name, syntax error, ...
				// The PyErr_Print will print it in rodsLog
				PyErr_Print();
				err_code = INVALID_OBJECT_TYPE; // not the best one but it exists
			}
		}
	}

    rei->status = err_code;
    return err_code;
}


int msiRodsPython2(msParam_t *irods_script_path, msParam_t *func_name, msParam_t *rule_name,
				   msParam_t *param1, msParam_t *param2, ruleExecInfo_t *rei)
{
	RE_TEST_MACRO( "    Calling msiRodsPython2");

	char *str_script_path = parseMspForStr(irods_script_path);
	char *str_func_name = parseMspForStr(func_name);
	char *str_rule_name = parseMspForStr(rule_name);
	char long_func_name[BIG_STR];
	bytesBuf_t inData;
	int status;
	rsComm_t *rsComm = rei->rsComm;
	PyObject *pModule, *pDict, *pFunc;
	int err_code = 0;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam1 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam1->msParamPtr = param1;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam2 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam2->msParamPtr = param2;

	// Create the ruleExecInfo_t variable to be used in python
	PyRuleExecInfo_t *pyRuleExecInfo = PyObject_NEW(PyRuleExecInfo_t,
													&PyRuleExecInfo_Type);
	pyRuleExecInfo->ruleExecInfoPtr = rei;

	// Code for recursion problem

	snprintf(long_func_name, BIG_STR, "%s(%s, %s)", str_func_name,
			 parseMspForStr(param1), parseMspForStr(param2));
	status = add_microservice_in_history(str_rule_name, long_func_name, pyRuleExecInfo);

	// If there is a recursion, status should be -1. This means that there's already
	// a call for the same rule, the same method, the same parameters and the
	// same rei
	if (status >= 0) {

		// Read the script from iRODS, get a string
		status = readFile(rsComm, str_script_path, &inData);
		if (status < 0) {
			rodsLogAndErrorMsg(LOG_ERROR, &rsComm->rError, status,
							   "%s:  could not read file, status = %d",
							   str_script_path, status);
			return USER_FILE_DOES_NOT_EXIST;
		}
		// Execute the script (It will load the functions defined in the script in
		// the global dictionary)
		char tmpStr[inData.len];
		snprintf(tmpStr, inData.len, "%s", (char *)inData.buf);
		err_code = PyRun_SimpleString(tmpStr);
		if (err_code == -1) {
			PyErr_Print();
			err_code = INVALID_OBJECT_TYPE;
			rei->status = err_code;
			return err_code;
		}
		// Get a reference to the main module and global dictionary
		pModule = PyImport_AddModule("__main__");
		pDict = PyModule_GetDict(pModule);
		// Get a reference to the function we want to call
		pFunc = PyDict_GetItemString(pDict, str_func_name);
		if (!pFunc) {
			PyErr_Print();
			err_code = NO_MICROSERVICE_FOUND_ERR;
			rei->status = err_code;
			return err_code;
		}

		// Call the python microservice with the parameter(s) and the rei
		PyObject *error_code = PyObject_CallFunction(pFunc, "OOO", pyMsParam1,
				pyMsParam2, pyRuleExecInfo);

		// If the user returns nothing, we assume there are no errors
		if (error_code == Py_None) {
			err_code = 0;
		} else {
			if (error_code)
				err_code = PyInt_AsLong(error_code);
			else {
				// if CallFunction fails error_code is NULL. This is an error in the
				// python script (wrong name, syntax error, ...
				// The PyErr_Print will print it in rodsLog
				PyErr_Print();
				err_code = INVALID_OBJECT_TYPE; // not the best one but it exists
			}
		}
	}

    rei->status = err_code;
    return err_code;
}


int msiRodsPython3(msParam_t *irods_script_path, msParam_t *func_name, msParam_t *rule_name,
				   msParam_t *param1, msParam_t *param2, msParam_t *param3,
				   ruleExecInfo_t *rei)
{
	RE_TEST_MACRO( "    Calling msiRodsPython3");

	char *str_script_path = parseMspForStr(irods_script_path);
	char *str_func_name = parseMspForStr(func_name);
	char *str_rule_name = parseMspForStr(rule_name);
	char long_func_name[BIG_STR];
	bytesBuf_t inData;
	int status;
	rsComm_t *rsComm = rei->rsComm;
	PyObject *pModule, *pDict, *pFunc;
	int err_code = 0;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam1 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam1->msParamPtr = param1;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam2 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam2->msParamPtr = param2;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam3 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam3->msParamPtr = param3;

	// Create the ruleExecInfo_t variable to be used in python
	PyRuleExecInfo_t *pyRuleExecInfo = PyObject_NEW(PyRuleExecInfo_t,
													&PyRuleExecInfo_Type);
	pyRuleExecInfo->ruleExecInfoPtr = rei;

	// Code for recursion problem

	snprintf(long_func_name, BIG_STR, "%s(%s, %s, %s)", str_func_name,
			 parseMspForStr(param1), parseMspForStr(param2), parseMspForStr(param3));
	status = add_microservice_in_history(str_rule_name, long_func_name, pyRuleExecInfo);

	// If there is a recursion, status should be -1. This means that there's already
	// a call for the same rule, the same method, the same parameters and the
	// same rei
	if (status >= 0) {

		// Read the script from iRODS, get a string
		status = readFile(rsComm, str_script_path, &inData);
		if (status < 0) {
			rodsLogAndErrorMsg(LOG_ERROR, &rsComm->rError, status,
							   "%s:  could not read file, status = %d",
							   str_script_path, status);
			return USER_FILE_DOES_NOT_EXIST;
		}
		// Execute the script (It will load the functions defined in the script in
		// the global dictionary)
		char tmpStr[inData.len];
		snprintf(tmpStr, inData.len, "%s", (char *)inData.buf);
		err_code = PyRun_SimpleString(tmpStr);
		if (err_code == -1) {
			PyErr_Print();
			err_code = INVALID_OBJECT_TYPE;
			rei->status = err_code;
			return err_code;
		}
		// Get a reference to the main module and global dictionary
		pModule = PyImport_AddModule("__main__");
		pDict = PyModule_GetDict(pModule);
		// Get a reference to the function we want to call
		pFunc = PyDict_GetItemString(pDict, str_func_name);
		if (!pFunc) {
			PyErr_Print();
			err_code = NO_MICROSERVICE_FOUND_ERR;
			rei->status = err_code;
			return err_code;
		}

		// Call the python microservice with the parameter(s) and the rei
		PyObject *error_code = PyObject_CallFunction(pFunc, "OOOO", pyMsParam1,
				pyMsParam2, pyMsParam3, pyRuleExecInfo);

		// If the user returns nothing, we assume there are no errors
		if (error_code == Py_None) {
			err_code = 0;
		} else {
			if (error_code)
				err_code = PyInt_AsLong(error_code);
			else {
				// if CallFunction fails error_code is NULL. This is an error in the
				// python script (wrong name, syntax error, ...
				// The PyErr_Print will print it in rodsLog
				PyErr_Print();
				err_code = INVALID_OBJECT_TYPE; // not the best one but it exists
			}
		}
	}

    rei->status = err_code;
    return err_code;
}


int msiRodsPython4(msParam_t *irods_script_path, msParam_t *func_name, msParam_t *rule_name,
				   msParam_t *param1, msParam_t *param2, msParam_t *param3,
				   msParam_t *param4, ruleExecInfo_t *rei)
{
	RE_TEST_MACRO( "    Calling msiRodsPython4");

	char *str_script_path = parseMspForStr(irods_script_path);
	char *str_func_name = parseMspForStr(func_name);
	char *str_rule_name = parseMspForStr(rule_name);
	char long_func_name[BIG_STR];
	bytesBuf_t inData;
	int status;
	rsComm_t *rsComm = rei->rsComm;
	PyObject *pModule, *pDict, *pFunc;
	int err_code = 0;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam1 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam1->msParamPtr = param1;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam2 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam2->msParamPtr = param2;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam3 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam3->msParamPtr = param3;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam4 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam4->msParamPtr = param4;

	// Create the ruleExecInfo_t variable to be used in python
	PyRuleExecInfo_t *pyRuleExecInfo = PyObject_NEW(PyRuleExecInfo_t,
													&PyRuleExecInfo_Type);
	pyRuleExecInfo->ruleExecInfoPtr = rei;

	// Code for recursion problem

	snprintf(long_func_name, BIG_STR, "%s(%s, %s, %s, %s)", str_func_name,
			 parseMspForStr(param1), parseMspForStr(param2), parseMspForStr(param3),
			 parseMspForStr(param4));
	status = add_microservice_in_history(str_rule_name, long_func_name, pyRuleExecInfo);

	// If there is a recursion, status should be -1. This means that there's already
	// a call for the same rule, the same method, the same parameters and the
	// same rei
	if (status >= 0) {

		// Read the script from iRODS, get a string
		status = readFile(rsComm, str_script_path, &inData);
		if (status < 0) {
			rodsLogAndErrorMsg(LOG_ERROR, &rsComm->rError, status,
							   "%s:  could not read file, status = %d",
							   str_script_path, status);
			return USER_FILE_DOES_NOT_EXIST;
		}
		// Execute the script (It will load the functions defined in the script in
		// the global dictionary)
		char tmpStr[inData.len];
		snprintf(tmpStr, inData.len, "%s", (char *)inData.buf);
		err_code = PyRun_SimpleString(tmpStr);
		if (err_code == -1) {
			PyErr_Print();
			err_code = INVALID_OBJECT_TYPE;
			rei->status = err_code;
			return err_code;
		}
		// Get a reference to the main module and global dictionary
		pModule = PyImport_AddModule("__main__");
		pDict = PyModule_GetDict(pModule);
		// Get a reference to the function we want to call
		pFunc = PyDict_GetItemString(pDict, str_func_name);
		if (!pFunc) {
			PyErr_Print();
			err_code = NO_MICROSERVICE_FOUND_ERR;
			rei->status = err_code;
			return err_code;
		}

		// Call the python microservice with the parameter(s) and the rei
		PyObject *error_code = PyObject_CallFunction(pFunc, "OOOOO", pyMsParam1,
				pyMsParam2, pyMsParam3, pyMsParam4, pyRuleExecInfo);

		// If the user returns nothing, we assume there are no errors
		if (error_code == Py_None) {
			err_code = 0;
		} else {
			if (error_code)
				err_code = PyInt_AsLong(error_code);
			else {
				// if CallFunction fails error_code is NULL. This is an error in the
				// python script (wrong name, syntax error, ...
				// The PyErr_Print will print it in rodsLog
				PyErr_Print();
				err_code = INVALID_OBJECT_TYPE; // not the best one but it exists
			}
		}
	}

    rei->status = err_code;
    return err_code;
}


int msiRodsPython5(msParam_t *irods_script_path, msParam_t *func_name, msParam_t *rule_name,
					msParam_t *param1, msParam_t *param2, msParam_t *param3,
					msParam_t *param4, msParam_t *param5, ruleExecInfo_t *rei)
{
	RE_TEST_MACRO( "    Calling msiRodsPython5");

	char *str_script_path = parseMspForStr(irods_script_path);
	char *str_func_name = parseMspForStr(func_name);
	char *str_rule_name = parseMspForStr(rule_name);
	char long_func_name[BIG_STR];
	bytesBuf_t inData;
	int status;
	rsComm_t *rsComm = rei->rsComm;
	PyObject *pModule, *pDict, *pFunc;
	int err_code = 0;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam1 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam1->msParamPtr = param1;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam2 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam2->msParamPtr = param2;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam3 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam3->msParamPtr = param3;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam4 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam4->msParamPtr = param4;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam5 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam5->msParamPtr = param5;

	// Create the ruleExecInfo_t variable to be used in python
	PyRuleExecInfo_t *pyRuleExecInfo = PyObject_NEW(PyRuleExecInfo_t, &PyRuleExecInfo_Type);
	pyRuleExecInfo->ruleExecInfoPtr = rei;

	// Code for recursion problem

	snprintf(long_func_name, BIG_STR, "%s(%s, %s, %s, %s, %s)", str_func_name,
			 parseMspForStr(param1), parseMspForStr(param2), parseMspForStr(param3),
			 parseMspForStr(param4), parseMspForStr(param5));
	status = add_microservice_in_history(str_rule_name, long_func_name, pyRuleExecInfo);

	// If there is a recursion, status should be -1. This means that there's already
	// a call for the same rule, the same method, the same parameters and the
	// same rei
	if (status >= 0) {

		// Read the script from iRODS, get a string
		status = readFile(rsComm, str_script_path, &inData);
		if (status < 0) {
			rodsLogAndErrorMsg(LOG_ERROR, &rsComm->rError, status,
							   "%s:  could not read file, status = %d",
							   str_script_path, status);
			return USER_FILE_DOES_NOT_EXIST;
		}
		// Execute the script (It will load the functions defined in the script in
		// the global dictionary)
		char tmpStr[inData.len];
		snprintf(tmpStr, inData.len, "%s", (char *)inData.buf);
		err_code = PyRun_SimpleString(tmpStr);
		if (err_code == -1) {
			PyErr_Print();
			err_code = INVALID_OBJECT_TYPE;
			rei->status = err_code;
			return err_code;
		}
		// Get a reference to the main module and global dictionary
		pModule = PyImport_AddModule("__main__");
		pDict = PyModule_GetDict(pModule);
		// Get a reference to the function we want to call
		pFunc = PyDict_GetItemString(pDict, str_func_name);
		if (!pFunc) {
			PyErr_Print();
			err_code = NO_MICROSERVICE_FOUND_ERR;
			rei->status = err_code;
			return err_code;
		}

		// Call the python microservice with the parameter(s) and the rei
		PyObject *error_code = PyObject_CallFunction(pFunc, "OOOOOO", pyMsParam1,
				pyMsParam2, pyMsParam3, pyMsParam4, pyMsParam5, pyRuleExecInfo);

		// If the user returns nothing, we assume there are no errors
		if (error_code == Py_None) {
			err_code = 0;
		} else {
			if (error_code)
				err_code = PyInt_AsLong(error_code);
			else {
				// if CallFunction fails error_code is NULL. This is an error in the
				// python script (wrong name, syntax error, ...
				// The PyErr_Print will print it in rodsLog
				PyErr_Print();
				err_code = INVALID_OBJECT_TYPE; // not the best one but it exists
			}
		}
	}

    rei->status = err_code;
    return err_code;
}


int msiRodsPython6(msParam_t *irods_script_path, msParam_t *func_name, msParam_t *rule_name,
				   msParam_t *param1, msParam_t *param2, msParam_t *param3,
				   msParam_t *param4, msParam_t *param5, msParam_t *param6,
				   ruleExecInfo_t *rei)
{
	RE_TEST_MACRO( "    Calling msiRodsPython6");

	char *str_script_path = parseMspForStr(irods_script_path);
	char *str_func_name = parseMspForStr(func_name);
	char *str_rule_name = parseMspForStr(rule_name);
	char long_func_name[BIG_STR];
	bytesBuf_t inData;
	int status;
	rsComm_t *rsComm = rei->rsComm;
	PyObject *pModule, *pDict, *pFunc;
	int err_code = 0;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam1 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam1->msParamPtr = param1;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam2 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam2->msParamPtr = param2;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam3 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam3->msParamPtr = param3;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam4 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam4->msParamPtr = param4;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam5 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam5->msParamPtr = param5;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam6 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam6->msParamPtr = param6;

	// Create the ruleExecInfo_t variable to be used in python
	PyRuleExecInfo_t *pyRuleExecInfo = PyObject_NEW(PyRuleExecInfo_t, &PyRuleExecInfo_Type);
	pyRuleExecInfo->ruleExecInfoPtr = rei;

	// Code for recursion problem

	snprintf(long_func_name, BIG_STR, "%s(%s, %s, %s, %s, %s, %s)", str_func_name,
			 parseMspForStr(param1), parseMspForStr(param2), parseMspForStr(param3),
			 parseMspForStr(param4), parseMspForStr(param5), parseMspForStr(param6));
	status = add_microservice_in_history(str_rule_name, long_func_name, pyRuleExecInfo);

	// If there is a recursion, status should be -1. This means that there's already
	// a call for the same rule, the same method, the same parameters and the
	// same rei
	if (status >= 0) {

		// Read the script from iRODS, get a string
		status = readFile(rsComm, str_script_path, &inData);
		if (status < 0) {
			rodsLogAndErrorMsg(LOG_ERROR, &rsComm->rError, status,
							   "%s:  could not read file, status = %d",
							   str_script_path, status);
			return USER_FILE_DOES_NOT_EXIST;
		}
		// Execute the script (It will load the functions defined in the script in
		// the global dictionary)
		char tmpStr[inData.len];
		snprintf(tmpStr, inData.len, "%s", (char *)inData.buf);
		err_code = PyRun_SimpleString(tmpStr);
		if (err_code == -1) {
			PyErr_Print();
			err_code = INVALID_OBJECT_TYPE;
			rei->status = err_code;
			return err_code;
		}
		// Get a reference to the main module and global dictionary
		pModule = PyImport_AddModule("__main__");
		pDict = PyModule_GetDict(pModule);
		// Get a reference to the function we want to call
		pFunc = PyDict_GetItemString(pDict, str_func_name);
		if (!pFunc) {
			PyErr_Print();
			err_code = NO_MICROSERVICE_FOUND_ERR;
			rei->status = err_code;
			return err_code;
		}

		// Call the python microservice with the parameter(s) and the rei
		PyObject *error_code = PyObject_CallFunction(pFunc, "OOOOOOO", pyMsParam1,
				pyMsParam2, pyMsParam3, pyMsParam4, pyMsParam5, pyMsParam6,
				pyRuleExecInfo);

		// If the user returns nothing, we assume there are no errors
		if (error_code == Py_None) {
			err_code = 0;
		} else {
			if (error_code)
				err_code = PyInt_AsLong(error_code);
			else {
				// if CallFunction fails error_code is NULL. This is an error in the
				// python script (wrong name, syntax error, ...
				// The PyErr_Print will print it in rodsLog
				PyErr_Print();
				err_code = INVALID_OBJECT_TYPE; // not the best one but it exists
			}
		}
	}

    rei->status = err_code;
    return err_code;
}


int msiRodsPython7(msParam_t *irods_script_path, msParam_t *func_name, msParam_t *rule_name,
				   msParam_t *param1, msParam_t *param2, msParam_t *param3,
				   msParam_t *param4, msParam_t *param5, msParam_t *param6,
				   msParam_t *param7, ruleExecInfo_t *rei)
{
	RE_TEST_MACRO( "    Calling msiRodsPython7");

	char *str_script_path = parseMspForStr(irods_script_path);
	char *str_func_name = parseMspForStr(func_name);
	char *str_rule_name = parseMspForStr(rule_name);
	char long_func_name[BIG_STR];
	bytesBuf_t inData;
	int status;
	rsComm_t *rsComm = rei->rsComm;
	PyObject *pModule, *pDict, *pFunc;
	int err_code = 0;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam1 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam1->msParamPtr = param1;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam2 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam2->msParamPtr = param2;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam3 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam3->msParamPtr = param3;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam4 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam4->msParamPtr = param4;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam5 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam5->msParamPtr = param5;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam6 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam6->msParamPtr = param6;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam7 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam7->msParamPtr = param7;

	// Create the ruleExecInfo_t variable to be used in python
	PyRuleExecInfo_t *pyRuleExecInfo = PyObject_NEW(PyRuleExecInfo_t, &PyRuleExecInfo_Type);
	pyRuleExecInfo->ruleExecInfoPtr = rei;

	// Code for recursion problem

	snprintf(long_func_name, BIG_STR, "%s(%s, %s, %s, %s, %s, %s, %s)", str_func_name,
			 parseMspForStr(param1), parseMspForStr(param2), parseMspForStr(param3),
			 parseMspForStr(param4), parseMspForStr(param5), parseMspForStr(param6),
			 parseMspForStr(param7));
	status = add_microservice_in_history(str_rule_name, long_func_name, pyRuleExecInfo);

	// If there is a recursion, status should be -1. This means that there's already
	// a call for the same rule, the same method, the same parameters and the
	// same rei
	if (status >= 0) {

		// Read the script from iRODS, get a string
		status = readFile(rsComm, str_script_path, &inData);
		if (status < 0) {
			rodsLogAndErrorMsg(LOG_ERROR, &rsComm->rError, status,
							   "%s:  could not read file, status = %d",
							   str_script_path, status);
			return USER_FILE_DOES_NOT_EXIST;
		}
		// Execute the script (It will load the functions defined in the script in
		// the global dictionary)
		char tmpStr[inData.len];
		snprintf(tmpStr, inData.len, "%s", (char *)inData.buf);
		err_code = PyRun_SimpleString(tmpStr);
		if (err_code == -1) {
			PyErr_Print();
			err_code = INVALID_OBJECT_TYPE;
			rei->status = err_code;
			return err_code;
		}
		// Get a reference to the main module and global dictionary
		pModule = PyImport_AddModule("__main__");
		pDict = PyModule_GetDict(pModule);
		// Get a reference to the function we want to call
		pFunc = PyDict_GetItemString(pDict, str_func_name);
		if (!pFunc) {
			PyErr_Print();
			err_code = NO_MICROSERVICE_FOUND_ERR;
			rei->status = err_code;
			return err_code;
		}

		// Call the python microservice with the parameter(s) and the rei
		PyObject *error_code = PyObject_CallFunction(pFunc, "OOOOOOOO", pyMsParam1,
				pyMsParam2, pyMsParam3, pyMsParam4, pyMsParam5, pyMsParam6,
				pyMsParam7, pyRuleExecInfo);

		// If the user returns nothing, we assume there are no errors
		if (error_code == Py_None) {
			err_code = 0;
		} else {
			if (error_code)
				err_code = PyInt_AsLong(error_code);
			else {
				// if CallFunction fails error_code is NULL. This is an error in the
				// python script (wrong name, syntax error, ...
				// The PyErr_Print will print it in rodsLog
				PyErr_Print();
				err_code = INVALID_OBJECT_TYPE; // not the best one but it exists
			}
		}
	}

    rei->status = err_code;
    return err_code;
}


int msiImportPythonZip(msParam_t *zip_path, ruleExecInfo_t *rei)
{
	RE_TEST_MACRO( "    Calling msiImportPythonZip");
	char *str_zip_path = parseMspForStr(zip_path);
	int err_code = 0;
	rsComm_t *rsComm = rei->rsComm;
	dataObjInp_t dataObjOprInp;

	portalOprOut_t *portalOprOut;
	bytesBuf_t dataObjOutBBuf;


	memset(&dataObjOprInp, 0, sizeof (dataObjInp_t));
	addKeyVal(&dataObjOprInp.condInput, FORCE_FLAG_KW, "");
	dataObjOprInp.openFlags = O_RDONLY;
	rstrcpy(dataObjOprInp.objPath, str_zip_path, MAX_NAME_LEN);

	rsDataObjGet(rsComm, &dataObjOprInp, &portalOprOut, &dataObjOutBBuf);


	FILE *f = NULL;
	char *tmp = strdup("/tmp/embedPythonXXXXXX");

	int fd;

	if ( ((fd = mkstemp(tmp)) == -1) || !(f = fdopen(fd, "w+"))) {
		printf ("Error\n");
		rei->status = 1;
		return err_code;
	}

	fwrite(dataObjOutBBuf.buf, 1, dataObjOutBBuf.len, f);
	fclose(f);

	printf("%s\n", tmp);



	char buffer[250];

	PyRun_SimpleString("import sys");
	sprintf(buffer, "sys.path.append('%s')", tmp);
	PyRun_SimpleString(buffer);

	sprintf(buffer, "imported_zip_packages.append('%s')", tmp);
	PyRun_SimpleString(buffer);



	unlink(tmp);



    rei->status = err_code;
    return err_code;
}


int msiLocalPython(msParam_t *script_path, msParam_t *func_name, msParam_t *rule_name,
				   ruleExecInfo_t *rei)
{
	RE_TEST_MACRO( "    Calling msiLocalPython");

	char buffer[250];
	char *str_script_path = parseMspForStr(script_path);
	char *str_func_name = parseMspForStr(func_name);
	char *str_rule_name = parseMspForStr(rule_name);
	char long_func_name[BIG_STR];
	int err_code = 0;
	int status;

	PyObject *pName, *pModule, *pDict, *pFunc;

	// Create the ruleExecInfo_t variable to be used in python
	PyRuleExecInfo_t *pyRuleExecInfo = PyObject_NEW(PyRuleExecInfo_t, &PyRuleExecInfo_Type);
	pyRuleExecInfo->ruleExecInfoPtr = rei;

	// Code for recursion problem

	snprintf(long_func_name, BIG_STR, "%s()", str_func_name);
	status = add_microservice_in_history(str_rule_name, long_func_name, pyRuleExecInfo);

	// If there is a recursion, status should be -1. This means that there's already
	// a call for the same rule, the same method, the same parameters and the
	// same rei
	if (status >= 0) {

		// Add the script passed in parameter in the sys.path list
		PyRun_SimpleString("import sys");
		sprintf(buffer, "sys.path.insert(0, '%s')", getBasename(str_script_path));
		PyRun_SimpleString(buffer);

		// Get the pointer of the function you want to call
		pName = PyString_FromString(getFilename(str_script_path));
		pModule = PyImport_Import(pName);
		if (!pModule) {
			PyErr_Print();
			err_code = USER_FILE_DOES_NOT_EXIST;
			rei->status = err_code;
			return err_code;
		}
		pDict = PyModule_GetDict(pModule);
		pFunc = PyDict_GetItemString(pDict, str_func_name);
		if (!pFunc) {
			PyErr_Print();
			err_code = NO_MICROSERVICE_FOUND_ERR;
			rei->status = err_code;
			return err_code;
		}
		// Call the python microservice with the parameter(s) and the rei
		PyObject *error_code = PyObject_CallFunction(pFunc, "O", pyRuleExecInfo);

		// If the user returns nothing, we assume there are no errors
		if (error_code == Py_None) {
			err_code = 0;
		} else {
			if (error_code)
				err_code = PyInt_AsLong(error_code);
			else {
				// if CallFunction fails error_code is NULL. This is an error in the
				// python script (wrong name, syntax error, ...
				// The PyErr_Print will print it in rodsLog
				PyErr_Print();
				err_code = INVALID_OBJECT_TYPE; // not the best one but it exists
			}
		}
	}

    rei->status = err_code;
    return err_code;
}


int msiLocalPython1(msParam_t *script_path, msParam_t *func_name, msParam_t *rule_name,
					msParam_t *param1, ruleExecInfo_t *rei)
{
	RE_TEST_MACRO( "    Calling msiLocalPython1");

	char buffer[250];
	char *str_script_path = parseMspForStr(script_path);
	char *str_func_name = parseMspForStr(func_name);
	char *str_rule_name = parseMspForStr(rule_name);
	char long_func_name[BIG_STR];
	int err_code = 0;
	int status;

	PyObject *pName, *pModule, *pDict, *pFunc;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam1 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam1->msParamPtr = param1;

	// Create the ruleExecInfo_t variable to be used in python
	PyRuleExecInfo_t *pyRuleExecInfo = PyObject_NEW(PyRuleExecInfo_t, &PyRuleExecInfo_Type);
	pyRuleExecInfo->ruleExecInfoPtr = rei;

	// Code for recursion problem

	snprintf(long_func_name, BIG_STR, "%s(%s)", str_func_name, parseMspForStr(param1));
	status = add_microservice_in_history(str_rule_name, long_func_name, pyRuleExecInfo);

	// If there is a recursion, status should be -1. This means that there's already
	// a call for the same rule, the same method, the same parameters and the
	// same rei
	if (status >= 0) {


		// Add the script passed in parameter in the sys.path list
		PyRun_SimpleString("import sys");
		sprintf(buffer, "sys.path.insert(0, '%s')", getBasename(str_script_path));
		PyRun_SimpleString(buffer);

		// Get the pointer of the function you want to call
		pName = PyString_FromString(getFilename(str_script_path));
		pModule = PyImport_Import(pName);
		if (!pModule) {
			PyErr_Print();
			err_code = USER_FILE_DOES_NOT_EXIST;
			rei->status = err_code;
			return err_code;
		}

	   pDict = PyModule_GetDict(pModule);
		pFunc = PyDict_GetItemString(pDict, str_func_name);
		if (!pFunc) {
			PyErr_Print();
			err_code = NO_MICROSERVICE_FOUND_ERR;
			rei->status = err_code;
			return err_code;
		}
		// Call the python microservice with the parameter(s) and the rei
		PyObject *error_code = PyObject_CallFunction(pFunc, "OO", pyMsParam1, pyRuleExecInfo);

		/// If the user returns nothing, we assume there are no errors
		if (error_code == Py_None) {
			err_code = 0;
		} else {
			if (error_code)
				err_code = PyInt_AsLong(error_code);
			else {
				// if CallFunction fails error_code is NULL. This is an error in the
				// python script (wrong name, syntax error, ...
				// The PyErr_Print will print it in rodsLog
				PyErr_Print();
				err_code = INVALID_OBJECT_TYPE; // not the best one but it exists
			}
		}
	}

    rei->status = err_code;
    return err_code;
}


int msiLocalPython2(msParam_t *script_path, msParam_t *func_name, msParam_t *rule_name,
					msParam_t *param1, msParam_t *param2, ruleExecInfo_t *rei)
{
	RE_TEST_MACRO( "    Calling msiLocalPython2");

	char buffer[250];
	char *str_script_path = parseMspForStr(script_path);
	char *str_func_name = parseMspForStr(func_name);
	char *str_rule_name = parseMspForStr(rule_name);
	char long_func_name[BIG_STR];
	int err_code = 0;
	int status;

	PyObject *pName, *pModule, *pDict, *pFunc;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam1 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam1->msParamPtr = param1;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam2 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam2->msParamPtr = param2;

	// Create the ruleExecInfo_t variable to be used in python
	PyRuleExecInfo_t *pyRuleExecInfo = PyObject_NEW(PyRuleExecInfo_t, &PyRuleExecInfo_Type);
	pyRuleExecInfo->ruleExecInfoPtr = rei;

	// Code for recursion problem

	snprintf(long_func_name, BIG_STR, "%s(%s, %s)", str_func_name, parseMspForStr(param1),
			parseMspForStr(param2));
	status = add_microservice_in_history(str_rule_name, long_func_name, pyRuleExecInfo);

	// If there is a recursion, status should be -1. This means that there's already
	// a call for the same rule, the same method, the same parameters and the
	// same rei
	if (status >= 0) {

		// Add the script passed in parameter in the sys.path list
		PyRun_SimpleString("import sys");
		sprintf(buffer, "sys.path.insert(0, '%s')", getBasename(str_script_path));
		PyRun_SimpleString(buffer);

		// Get the pointer of the function you want to call
		pName = PyString_FromString(getFilename(str_script_path));
		pModule = PyImport_Import(pName);
		if (!pModule) {
			PyErr_Print();
			err_code = USER_FILE_DOES_NOT_EXIST;
			rei->status = err_code;
			return err_code;
		}
		pDict = PyModule_GetDict(pModule);
		pFunc = PyDict_GetItemString(pDict, str_func_name);
		if (!pFunc) {
			PyErr_Print();
			err_code = NO_MICROSERVICE_FOUND_ERR;
			rei->status = err_code;
			return err_code;
		}

		// Call the python microservice with the parameter(s) and the rei
		PyObject *error_code = PyObject_CallFunction(pFunc, "OOO", pyMsParam1,
				pyMsParam2, pyRuleExecInfo);

		// If the user returns nothing, we assume there are no errors
		if (error_code == Py_None) {
			err_code = 0;
		} else {
			if (error_code)
				err_code = PyInt_AsLong(error_code);
			else {
				// if CallFunction fails error_code is NULL. This is an error in the
				// python script (wrong name, syntax error, ...
				// The PyErr_Print will print it in rodsLog
				PyErr_Print();
				err_code = INVALID_OBJECT_TYPE; // not the best one but it exists
			}
		}
	}

    rei->status = err_code;
    return err_code;
}


int msiLocalPython3(msParam_t *script_path, msParam_t *func_name, msParam_t *rule_name,
					msParam_t *param1, msParam_t *param2, msParam_t *param3,
					ruleExecInfo_t *rei)
{
	RE_TEST_MACRO( "    Calling msiLocalPython3");

	char buffer[250];
	char *str_script_path = parseMspForStr(script_path);
	char *str_func_name = parseMspForStr(func_name);
	char *str_rule_name = parseMspForStr(rule_name);
	char long_func_name[BIG_STR];
	int err_code = 0;
	int status;

	PyObject *pName, *pModule, *pDict, *pFunc;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam1 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam1->msParamPtr = param1;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam2 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam2->msParamPtr = param2;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam3 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam3->msParamPtr = param3;

	// Create the ruleExecInfo_t variable to be used in python
	PyRuleExecInfo_t *pyRuleExecInfo = PyObject_NEW(PyRuleExecInfo_t, &PyRuleExecInfo_Type);
	pyRuleExecInfo->ruleExecInfoPtr = rei;

	// Code for recursion problem

	snprintf(long_func_name, BIG_STR, "%s(%s, %s, %s)", str_func_name,
			 parseMspForStr(param1), parseMspForStr(param2), parseMspForStr(param3));
	status = add_microservice_in_history(str_rule_name, long_func_name, pyRuleExecInfo);

	// If there is a recursion, status should be -1. This means that there's already
	// a call for the same rule, the same method, the same parameters and the
	// same rei
	if (status >= 0) {

		// Add the script passed in parameter in the sys.path list
		PyRun_SimpleString("import sys");
		sprintf(buffer, "sys.path.insert(0, '%s')", getBasename(str_script_path));
		PyRun_SimpleString(buffer);

		// Get the pointer of the function you want to call
		pName = PyString_FromString(getFilename(str_script_path));
		pModule = PyImport_Import(pName);
		if (!pModule) {
			PyErr_Print();
			err_code = USER_FILE_DOES_NOT_EXIST;
			rei->status = err_code;
			return err_code;
		}
		pDict = PyModule_GetDict(pModule);
		pFunc = PyDict_GetItemString(pDict, str_func_name);
		if (!pFunc) {
			PyErr_Print();
			err_code = NO_MICROSERVICE_FOUND_ERR;
			rei->status = err_code;
			return err_code;
		}

		// Call the python microservice with the parameter(s) and the rei
		PyObject *error_code = PyObject_CallFunction(pFunc, "OOOO", pyMsParam1,
				pyMsParam2, pyMsParam3, pyRuleExecInfo);

		// If the user returns nothing, we assume there are no errors
		if (error_code == Py_None) {
			err_code = 0;
		} else {
			if (error_code)
				err_code = PyInt_AsLong(error_code);
			else {
				// if CallFunction fails error_code is NULL. This is an error in the
				// python script (wrong name, syntax error, ...
				// The PyErr_Print will print it in rodsLog
				PyErr_Print();
				err_code = INVALID_OBJECT_TYPE; // not the best one but it exists
			}
		}
	}

    rei->status = err_code;
    return err_code;
}


int msiLocalPython4(msParam_t *script_path, msParam_t *func_name, msParam_t *rule_name,
					msParam_t *param1, msParam_t *param2, msParam_t *param3,
					msParam_t *param4, ruleExecInfo_t *rei)
{
	RE_TEST_MACRO( "    Calling msiLocalPython4");

	char buffer[250];
	char *str_script_path = parseMspForStr(script_path);
	char *str_func_name = parseMspForStr(func_name);
	char *str_rule_name = parseMspForStr(rule_name);
	char long_func_name[BIG_STR];
	int err_code = 0;
	int status;

	PyObject *pName, *pModule, *pDict, *pFunc;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam1 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam1->msParamPtr = param1;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam2 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam2->msParamPtr = param2;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam3 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam3->msParamPtr = param3;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam4 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam4->msParamPtr = param4;

	// Create the ruleExecInfo_t variable to be used in python
	PyRuleExecInfo_t *pyRuleExecInfo = PyObject_NEW(PyRuleExecInfo_t, &PyRuleExecInfo_Type);
	pyRuleExecInfo->ruleExecInfoPtr = rei;

	// Code for recursion problem

	snprintf(long_func_name, BIG_STR, "%s(%s, %s, %s, %s)", str_func_name,
			 parseMspForStr(param1), parseMspForStr(param2), parseMspForStr(param3),
			 parseMspForStr(param4));
	status = add_microservice_in_history(str_rule_name, long_func_name, pyRuleExecInfo);

	// If there is a recursion, status should be -1. This means that there's already
	// a call for the same rule, the same method, the same parameters and the
	// same rei
	if (status >= 0) {

		// Add the script passed in parameter in the sys.path list
		PyRun_SimpleString("import sys");
		sprintf(buffer, "sys.path.insert(0, '%s')", getBasename(str_script_path));
		PyRun_SimpleString(buffer);

		// Get the pointer of the function you want to call
		pName = PyString_FromString(getFilename(str_script_path));
		pModule = PyImport_Import(pName);
		if (!pModule) {
			PyErr_Print();
			err_code = USER_FILE_DOES_NOT_EXIST;
			rei->status = err_code;
			return err_code;
		}
		pDict = PyModule_GetDict(pModule);
		pFunc = PyDict_GetItemString(pDict, str_func_name);
		if (!pFunc) {
			PyErr_Print();
			err_code = NO_MICROSERVICE_FOUND_ERR;
			rei->status = err_code;
			return err_code;
		}

		// Call the python microservice with the parameter(s) and the rei
		PyObject *error_code = PyObject_CallFunction(pFunc, "OOOOO",pyMsParam1,
													 pyMsParam2, pyMsParam3, pyMsParam4, pyRuleExecInfo);

		// If the user returns nothing, we assume there are no errors
		if (error_code == Py_None) {
			err_code = 0;
		} else {
			if (error_code)
				err_code = PyInt_AsLong(error_code);
			else {
				// if CallFunction fails error_code is NULL. This is an error in the
				// python script (wrong name, syntax error, ...
				// The PyErr_Print will print it in rodsLog
				PyErr_Print();
				err_code = INVALID_OBJECT_TYPE; // not the best one but it exists
			}
		}
	}

    rei->status = err_code;
    return err_code;
}


int msiLocalPython5(msParam_t *script_path, msParam_t *func_name, msParam_t *rule_name,
					msParam_t *param1, msParam_t *param2, msParam_t *param3,
					msParam_t *param4, msParam_t *param5, ruleExecInfo_t *rei)
{
	RE_TEST_MACRO( "    Calling msiLocalPython5");

	char buffer[250];
	char *str_script_path = parseMspForStr(script_path);
	char *str_func_name = parseMspForStr(func_name);
	char *str_rule_name = parseMspForStr(rule_name);
	char long_func_name[BIG_STR];
	int err_code = 0;
	int status;

	PyObject *pName, *pModule, *pDict, *pFunc;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam1 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam1->msParamPtr = param1;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam2 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam2->msParamPtr = param2;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam3 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam3->msParamPtr = param3;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam4 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam4->msParamPtr = param4;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam5 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam5->msParamPtr = param5;

	// Create the ruleExecInfo_t variable to be used in python
	PyRuleExecInfo_t *pyRuleExecInfo = PyObject_NEW(PyRuleExecInfo_t, &PyRuleExecInfo_Type);
	pyRuleExecInfo->ruleExecInfoPtr = rei;

	// Code for recursion problem

	snprintf(long_func_name, BIG_STR, "%s(%s, %s, %s, %s, %s)", str_func_name,
			 parseMspForStr(param1), parseMspForStr(param2), parseMspForStr(param3),
			 parseMspForStr(param4), parseMspForStr(param5));
	status = add_microservice_in_history(str_rule_name, long_func_name, pyRuleExecInfo);

	// If there is a recursion, status should be -1. This means that there's already
	// a call for the same rule, the same method, the same parameters and the
	// same rei
	if (status >= 0) {
		// Add the script passed in parameter in the sys.path list
		PyRun_SimpleString("import sys");
		sprintf(buffer, "sys.path.insert(0, '%s')", getBasename(str_script_path));
		PyRun_SimpleString(buffer);

		// Get the pointer of the function you want to call
		pName = PyString_FromString(getFilename(str_script_path));
		pModule = PyImport_Import(pName);
		if (!pModule) {
			PyErr_Print();
			err_code = USER_FILE_DOES_NOT_EXIST;
			rei->status = err_code;
			return err_code;
		}
		pDict = PyModule_GetDict(pModule);
		pFunc = PyDict_GetItemString(pDict, str_func_name);
		if (!pFunc) {
			PyErr_Print();
			err_code = NO_MICROSERVICE_FOUND_ERR;
			rei->status = err_code;
			return err_code;
		}

		// Call the python microservice with the parameter(s) and the rei
		PyObject *error_code = PyObject_CallFunction(pFunc, "OOOOOO", pyMsParam1,
				pyMsParam2, pyMsParam3, pyMsParam4, pyMsParam5, pyRuleExecInfo);

		// If the user returns nothing, we assume there are no errors
		if (error_code == Py_None) {
			err_code = 0;
		} else {
			if (error_code)
				err_code = PyInt_AsLong(error_code);
			else {
				// if CallFunction fails error_code is NULL. This is an error in the
				// python script (wrong name, syntax error, ...
				// The PyErr_Print will print it in rodsLog
				PyErr_Print();
				err_code = INVALID_OBJECT_TYPE; // not the best one but it exists
			}
		}
	}

    rei->status = err_code;
    return err_code;
}


int msiLocalPython6(msParam_t *script_path, msParam_t *func_name, msParam_t *rule_name,
					msParam_t *param1, msParam_t *param2, msParam_t *param3,
					msParam_t *param4, msParam_t *param5, msParam_t *param6,
					ruleExecInfo_t *rei)
{
	RE_TEST_MACRO( "    Calling msiLocalPython6");


	char buffer[250];
	char *str_script_path = parseMspForStr(script_path);
	char *str_func_name = parseMspForStr(func_name);
	char *str_rule_name = parseMspForStr(rule_name);
	char long_func_name[BIG_STR];
	int err_code = 0;
	int status;

	PyObject *pName, *pModule, *pDict, *pFunc;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam1 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam1->msParamPtr = param1;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam2 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam2->msParamPtr = param2;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam3 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam3->msParamPtr = param3;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam4 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam4->msParamPtr = param4;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam5 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam5->msParamPtr = param5;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam6 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam6->msParamPtr = param6;

	// Create the ruleExecInfo_t variable to be used in python
	PyRuleExecInfo_t *pyRuleExecInfo = PyObject_NEW(PyRuleExecInfo_t, &PyRuleExecInfo_Type);
	pyRuleExecInfo->ruleExecInfoPtr = rei;

	// Code for recursion problem

	snprintf(long_func_name, BIG_STR, "%s(%s, %s, %s, %s, %s, %s)", str_func_name,
			 parseMspForStr(param1), parseMspForStr(param2), parseMspForStr(param3),
			 parseMspForStr(param4), parseMspForStr(param5), parseMspForStr(param6));
	status = add_microservice_in_history(str_rule_name, long_func_name, pyRuleExecInfo);

	// If there is a recursion, status should be -1. This means that there's already
	// a call for the same rule, the same method, the same parameters and the
	// same rei
	if (status >= 0) {
		// Add the script passed in parameter in the sys.path list
		PyRun_SimpleString("import sys");
		sprintf(buffer, "sys.path.insert(0, '%s')", getBasename(str_script_path));
		PyRun_SimpleString(buffer);

		// Get the pointer of the function you want to call
		pName = PyString_FromString(getFilename(str_script_path));
		pModule = PyImport_Import(pName);
		if (!pModule) {
			PyErr_Print();
			err_code = USER_FILE_DOES_NOT_EXIST;
			rei->status = err_code;
			return err_code;
		}
		pDict = PyModule_GetDict(pModule);
		pFunc = PyDict_GetItemString(pDict, str_func_name);
		if (!pFunc) {
			PyErr_Print();
			err_code = NO_MICROSERVICE_FOUND_ERR;
			rei->status = err_code;
			return err_code;
		}

		// Call the python microservice with the parameter(s) and the rei
		PyObject *error_code = PyObject_CallFunction(pFunc, "OOOOOOO", pyMsParam1,
				pyMsParam2, pyMsParam3, pyMsParam4, pyMsParam5, pyMsParam6,
				pyRuleExecInfo);

		// If the user returns nothing, we assume there are no errors
		if (error_code == Py_None) {
			err_code = 0;
		} else {
			if (error_code)
				err_code = PyInt_AsLong(error_code);
			else {
				// if CallFunction fails error_code is NULL. This is an error in the
				// python script (wrong name, syntax error, ...
				// The PyErr_Print will print it in rodsLog
				PyErr_Print();
				err_code = INVALID_OBJECT_TYPE; // not the best one but it exists
			}
		}
	}

    rei->status = err_code;
    return err_code;
}


int msiLocalPython7(msParam_t *script_path, msParam_t *func_name, msParam_t *rule_name,
					msParam_t *param1, msParam_t *param2, msParam_t *param3,
					msParam_t *param4, msParam_t *param5, msParam_t *param6,
					msParam_t *param7, ruleExecInfo_t *rei)
{
	RE_TEST_MACRO( "    Calling msiLocalPython7");

	char buffer[250];
	char *str_script_path = parseMspForStr(script_path);
	char *str_func_name = parseMspForStr(func_name);
	char *str_rule_name = parseMspForStr(rule_name);
	char long_func_name[BIG_STR];
	int err_code = 0;
	int status;

	PyObject *pName, *pModule, *pDict, *pFunc;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam1 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam1->msParamPtr = param1;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam2 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam2->msParamPtr = param2;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam3 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam3->msParamPtr = param3;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam4 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam4->msParamPtr = param4;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam5 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam5->msParamPtr = param5;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam6 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam6->msParamPtr = param6;

	// Create a msParam_t variable to be used in python
	PyMsParam_t *pyMsParam7 = PyObject_NEW(PyMsParam_t, &PyMsParam_Type);
	pyMsParam7->msParamPtr = param7;

	// Create the ruleExecInfo_t variable to be used in python
	PyRuleExecInfo_t *pyRuleExecInfo = PyObject_NEW(PyRuleExecInfo_t, &PyRuleExecInfo_Type);
	pyRuleExecInfo->ruleExecInfoPtr = rei;

	// Code for recursion problem

	snprintf(long_func_name, BIG_STR, "%s(%s, %s, %s, %s, %s, %s, %s)", str_func_name,
			 parseMspForStr(param1), parseMspForStr(param2), parseMspForStr(param3),
			 parseMspForStr(param4), parseMspForStr(param5), parseMspForStr(param6),
			 parseMspForStr(param7));
	status = add_microservice_in_history(str_rule_name, long_func_name, pyRuleExecInfo);

	// If there is a recursion, status should be -1. This means that there's already
	// a call for the same rule, the same method, the same parameters and the
	// same rei
	if (status >= 0) {
		// Add the script passed in parameter in the sys.path list
		PyRun_SimpleString("import sys");
		sprintf(buffer, "sys.path.insert(0, '%s')", getBasename(str_script_path));
		PyRun_SimpleString(buffer);

		// Get the pointer of the function you want to call
		pName = PyString_FromString(getFilename(str_script_path));
		pModule = PyImport_Import(pName);
		if (!pModule) {
			PyErr_Print();
			err_code = USER_FILE_DOES_NOT_EXIST;
			rei->status = err_code;
			return err_code;
		}
		pDict = PyModule_GetDict(pModule);
		pFunc = PyDict_GetItemString(pDict, str_func_name);
		if (!pFunc) {
			PyErr_Print();
			err_code = NO_MICROSERVICE_FOUND_ERR;
			rei->status = err_code;
			return err_code;
		}

		// Call the python microservice with the parameter(s) and the rei
		PyObject *error_code = PyObject_CallFunction(pFunc, "OOOOOOOO", pyMsParam1,
				pyMsParam2, pyMsParam3, pyMsParam4, pyMsParam5, pyMsParam6,
				pyMsParam7, pyRuleExecInfo);

		// If the user returns nothing, we assume there are no errors
		if (error_code == Py_None) {
			err_code = 0;
		} else {
			if (error_code)
				err_code = PyInt_AsLong(error_code);
			else {
				// if CallFunction fails error_code is NULL. This is an error in the
				// python script (wrong name, syntax error, ...
				// The PyErr_Print will print it in rodsLog
				PyErr_Print();
				err_code = INVALID_OBJECT_TYPE; // not the best one but it exists
			}
		}
	}

    rei->status = err_code;
    return err_code;
}


int msiExecPython(msParam_t *python_code, ruleExecInfo_t *rei) {
	char *python_code_str = parseMspForStr(python_code);
	int status = PyRun_SimpleString(python_code_str);
	return status;
}


int msiPyInitialize(ruleExecInfo_t *rei) {
	// Initialize the python interpreter
	if (!Py_IsInitialized()) {
		Py_Initialize();
		PyRun_SimpleString("call_history = {}");
		PyRun_SimpleString("imported_zip_packages = []");
	}
	return 0;
}


int msiPyFinalize(ruleExecInfo_t *rei) {
	// Undo all initializations made by Py_Initialize()
	// There's a bug in python 2 when you initialize/finalize the interpreter
	// several times in the same process. This happens for the hooks which
	// are dealt by the same irodsAgent process.
	// Without finalize, I hope that when the thread is disposed of, the memory
	// is cleaned.
	//Py_Finalize();

	PyRun_SimpleString("import os");
	PyRun_SimpleString("for f in imported_zip_packages:os.remove(f)");


	return 0;
}



