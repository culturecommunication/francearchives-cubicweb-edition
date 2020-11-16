/*
 * Copyright © LOGILAB S.A. (Paris, FRANCE) 2016-2019
 * Contact http://www.logilab.fr -- mailto:contact@logilab.fr
 *
 * This software is governed by the CeCILL-C license under French law and
 * abiding by the rules of distribution of free software. You can use,
 * modify and/ or redistribute the software under the terms of the CeCILL-C
 * license as circulated by CEA, CNRS and INRIA at the following URL
 * "http://www.cecill.info".
 *
 * As a counterpart to the access to the source code and rights to copy,
 * modify and redistribute granted by the license, users are provided only
 * with a limited warranty and the software's author, the holder of the
 * economic rights, and the successive licensors have only limited liability.
 *
 * In this respect, the user's attention is drawn to the risks associated
 * with loading, using, modifying and/or developing or reproducing the
 * software by the user in light of its specific status of free software,
 * that may mean that it is complicated to manipulate, and that also
 * therefore means that it is reserved for developers and experienced
 * professionals having in-depth computer knowledge. Users are therefore
 * encouraged to load and test the software's suitability as regards their
 * requirements in conditions enabling the security of their systemsand/or
 * data to be ensured and, more generally, to use and operate it in the
 * same conditions as regards security.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL-C license and that you accept its terms.
 */
/* global fetch */

const {
    default: {buildUrl},
} = require('../api')

const {CALL_FETCH} = require('../utils/api')

const HTTP_RE = /https?/

function callApi(url, fetchopts) {
    if (!HTTP_RE.test(url)) {
        url = buildUrl(url)
    }
    const opts = Object.assign(
        {
            credentials: 'same-origin',
            headers: {
                Accept: 'application/json',
                'Content-Type': 'application/json',
            },
        },
        fetchopts,
    )
    return fetch(url, opts).then(response =>
        response.json().then(json => ({json, response})),
    )
}

const middleware = store => next => action => {
    const callApiDetails = action[CALL_FETCH]
    if (callApiDetails === undefined) {
        return next(action)
    }
    const {endpoint, types, fetchopts, shouldMakeRequest} = callApiDetails,
        {
            requestActionType,
            successActionType,
            failureActionType,
            notNecessaryActionType,
        } = types,
        {payload} = action
    next({type: requestActionType, payload})
    if (!shouldMakeRequest(store.getState())) {
        return next({type: notNecessaryActionType, payload})
    }
    return callApi(endpoint, fetchopts).then(
        response =>
            next({
                payload,
                type: successActionType,
                response,
            }),
        error =>
            next({
                type: failureActionType,
                error: error.message,
                payload,
            }),
    )
}

module.exports = {
    middleware,
}
