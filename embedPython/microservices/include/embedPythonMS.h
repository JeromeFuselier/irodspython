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


#ifndef EMBEDPYTHONMS_H
#define EMBEDPYTHONMS_H

#include "apiHeaderAll.h"
#include "objMetaOpr.h"
#include "miscUtil.h"
/*
#include "rods.h"
#include "reGlobalsExtern.h"
#include "rsGlobalExtern.h"
#include "rcGlobalExtern.h"*/


int msiRodsPython(msParam_t *script_path, msParam_t *func_name, msParam_t *rule_name,
				  ruleExecInfo_t *rei);
int msiRodsPython1(msParam_t *script_path, msParam_t *func_name, msParam_t *rule_name,
				   msParam_t *param1, ruleExecInfo_t *rei);
int msiRodsPython2(msParam_t *script_path, msParam_t *func_name, msParam_t *rule_name,
				   msParam_t *param1, msParam_t *param2, ruleExecInfo_t *rei);
int msiRodsPython3(msParam_t *script_path, msParam_t *func_name, msParam_t *rule_name,
				   msParam_t *param1, msParam_t *param2, msParam_t *param3,
				   ruleExecInfo_t *rei);
int msiRodsPython4(msParam_t *script_path, msParam_t *func_name, msParam_t *rule_name,
				   msParam_t *param1, msParam_t *param2, msParam_t *param3,
				   msParam_t *param4, ruleExecInfo_t *rei);
int msiRodsPython5(msParam_t *script_path, msParam_t *func_name, msParam_t *rule_name,
				   msParam_t *param1, msParam_t *param2, msParam_t *param3,
				   msParam_t *param4, msParam_t *param5, ruleExecInfo_t *rei);
int msiRodsPython6(msParam_t *script_path, msParam_t *func_name, msParam_t *rule_name,
		           msParam_t *param1, msParam_t *param2, msParam_t *param3,
		           msParam_t *param4, msParam_t *param5, msParam_t *param6,
		           ruleExecInfo_t *rei);
int msiRodsPython7(msParam_t *script_path, msParam_t *func_name, msParam_t *rule_name,
		           msParam_t *param1, msParam_t *param2, msParam_t *param3,
		           msParam_t *param4, msParam_t *param5, msParam_t *param6,
		           msParam_t *param7, ruleExecInfo_t *rei);

int msiLocalPython(msParam_t *irods_script_path, msParam_t *func_name,
				   msParam_t *rule_name, ruleExecInfo_t *rei);
int msiLocalPython1(msParam_t *irods_script_path, msParam_t *func_name,
					msParam_t *rule_name, msParam_t *param1, ruleExecInfo_t *rei);
int msiLocalPython2(msParam_t *irods_script_path, msParam_t *func_name,
					msParam_t *rule_name, msParam_t *param1, msParam_t *param2,
					ruleExecInfo_t *rei);
int msiLocalPython3(msParam_t *irods_script_path, msParam_t *func_name,
					msParam_t *rule_name, msParam_t *param1, msParam_t *param2,
					msParam_t *param3, ruleExecInfo_t *rei);
int msiLocalPython4(msParam_t *irods_script_path, msParam_t *func_name,
					msParam_t *rule_name, msParam_t *param1, msParam_t *param2,
					msParam_t *param3, msParam_t *param4, ruleExecInfo_t *rei);
int msiLocalPython5(msParam_t *irods_script_path, msParam_t *func_name,
					msParam_t *rule_name, msParam_t *param1, msParam_t *param2,
					msParam_t *param3, msParam_t *param4, msParam_t *param5,
					ruleExecInfo_t *rei);
int msiLocalPython6(msParam_t *irods_script_path, msParam_t *func_name,
					msParam_t *rule_name, msParam_t *param1, msParam_t *param2,
					msParam_t *param3, msParam_t *param4, msParam_t *param5,
					msParam_t *param6, ruleExecInfo_t *rei);
int msiLocalPython7(msParam_t *irods_script_path, msParam_t *func_name,
					msParam_t *rule_name, msParam_t *param1, msParam_t *param2,
					msParam_t *param3, msParam_t *param4, msParam_t *param5,
					msParam_t *param6, msParam_t *param7, ruleExecInfo_t *rei);

int msiExecPython(msParam_t *python_code, ruleExecInfo_t *rei);

int msiPyInitialize(ruleExecInfo_t *rei);
int msiPyFinalize(ruleExecInfo_t *rei);

int msiImportPythonZip(msParam_t *zip_path, ruleExecInfo_t *rei);

#endif	/* EMBEDPYTHONMS_H */
