#ifndef __OPENSEARCH_ENLIST_H__
#define __OPENSEARCH_ENLIST_H__

#ifdef __cplusplus
extern "C" {
#endif
#ifdef WIN32
#ifdef _HANDLE_ENLIST_IN_DTC_

#undef DLL_DECLARE
#ifdef _OPENSEARCH_ENLIST_FUNCS_IMPLEMENT_
#define DLL_DECLARE _declspec(dllexport)
#else
#ifdef _OPENSEARCH_ENLIST_FUNCS_IMPORT_
#define DLL_DECLARE _declspec(dllimport)
#else
#define DLL_DECLARE
#endif /* _OPENSEARCH_ENLIST_FUNCS_IMPORT_ */
#endif /* _OPENSEARCH_ENLIST_FUNCS_IMPLEMENT_ */

RETCODE EnlistInDtc(void *conn, void *pTra, int method);
RETCODE DtcOnDisconnect(void *);
RETCODE IsolateDtcConn(void *, BOOL continueConnection);
//	for testing
DLL_DECLARE void *GetTransactionObject(HRESULT *hres);
DLL_DECLARE void ReleaseTransactionObject(void *);

#endif /* _HANDLE_ENLIST_IN_DTC_ */
#endif /* WIN32 */

#ifdef __cplusplus
}
#endif
#endif /* __OPENSEARCH_ENLIST_H__ */
