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
/* global SCRIPT_NAME */

import 'whatwg-fetch'
import {isEmpty} from 'lodash/lang'
import {get, defaultsDeep} from 'lodash/object'
import {each} from 'lodash/collection'

function buildUrl(path) {
    const uri = path[0] === '/' ? path.slice(1) : path
    const prefix =
        SCRIPT_NAME.slice(-1) === '/' ? SCRIPT_NAME : `${SCRIPT_NAME}/`
    return prefix + uri
}

function jsonFetch(url, options = {}) {
    const fullUrl = buildUrl(url)
    defaultsDeep(options, {
        credentials: 'same-origin',
        headers: {Accept: 'application/json'},
    })
    return fetch(fullUrl, options).then((response) => {
        const {headers} = response,
            contentLength = headers.get('Content-Length'),
            noContent = contentLength === '0' || contentLength === 0
        if (noContent && response.status >= 200 && response.status < 400) {
            // empty body but status seems ok, so don't try to parse body as json
            // but don't throw any error
            return response
        }
        if (
            response.headers.get('content-type') &&
            response.headers
                .get('content-type')
                .toLowerCase()
                .indexOf('application/json') >= 0
        ) {
            return response.json()
        }
        const method = options.method || 'GET'
        throw new Error(
            `Got "${response.statusText}" from ${method} request at ${fullUrl}`,
        )
    })
}

function jsonDeleteFetch(url, options = {}) {
    // like jsonFetch but Accept do not default to application/json
    // which will make pyramid route unselectable (due to competition with others)
    const fullUrl = buildUrl(url)
    defaultsDeep(options, {credentials: 'same-origin', method: 'DELETE'})
    return fetch(fullUrl, options).then((response) => {
        if (
            response.headers.get('content-type') &&
            response.headers
                .get('content-type')
                .toLowerCase()
                .indexOf('application/json') >= 0
        ) {
            return response.json()
        }
        if (response.status === 204) {
            return response
        }
        const method = options.method || 'GET'
        throw new Error(
            `Got "${response.statusText}" from ${method} request at ${fullUrl}`,
        )
    })
}

function jsonSchemaFetch(url, options = {}) {
    defaultsDeep(options, {headers: {Accept: 'application/schema+json'}})
    return jsonFetch(url, options)
}

function jsonFetchCollection(url, options) {
    // Filter out error response by returning and empty collection.
    return jsonFetch(url, options).then((doc) => {
        if (Object.prototype.hasOwnProperty.call(doc, 'errors')) {
            console.error(doc)
            return []
        }
        return Array.isArray(doc) ? doc : doc.data
    })
}

function getSchema(etype, eid = null, role = null, schema_type = null) {
    var url = `/${etype}`,
        args = []
    if (eid !== null) {
        url += `/${eid}`
    }
    url += '/schema'
    if (role !== null) {
        args.push(`role=${role}`)
    }
    if (schema_type !== null) {
        args.push(`schema_type=${schema_type}`)
    }
    if (args.length > 0) {
        url += '?' + args.join('&')
    }
    const options = {
        headers: {
            Accept: 'application/schema+json',
        },
    }
    return jsonFetch(url, options)
}

function getUiSchema(etype, schemaType = null) {
    let url = `/${etype}/uischema`
    if (schemaType !== null) {
        url = `${url}?schema_type=${schemaType}`
    }
    return jsonFetch(url)
}

function getRelatedSchema(etype, rtype, role = 'creation', targetType = null) {
    let url = `/${etype}/relationships/${rtype}/schema?role=${role}`
    if (targetType !== null) {
        url += `&target_type=${targetType}`
    }
    const options = {
        headers: {
            Accept: 'application/schema+json',
        },
    }
    return jsonFetch(url, options)
}

function getRelatedUiSchema(etype, rtype, targetType = null) {
    let url = `/${etype}/relationships/${rtype}/uischema`
    if (targetType) {
        url += `?target_type=${targetType}`
    }
    return jsonFetch(url)
}

function getEntities(etype, attrs = []) {
    let url = `/${etype}/`
    if (attrs.length) {
        url = `${url}?attrs=${attrs.join(',')}`
    }
    return jsonFetchCollection(url)
}

function getRelated(etype, eid, rtype, params = {}) {
    const url = `/${etype}/${eid}/${rtype}`
    const searchParams = new URLSearchParams()
    if (!isEmpty(params)) {
        const sort = get(params, 'sort', null)
        if (sort !== null) {
            searchParams.append('sort', sort)
        }
        const targetType = get(params, 'targetType', null)
        if (targetType !== null) {
            searchParams.append('target_type', targetType)
        }
    }
    return jsonFetchCollection(`${url}?${searchParams.toString()}`)
}

function getAvailableTargets(etype, rtype, eid = null, q = null, params = {}) {
    let url = null
    const searchParams = new URLSearchParams()
    if (eid !== null) {
        url = `${etype}/${eid}/relationships/${rtype}/available-targets`
        searchParams.append('eid', eid)
    } else {
        url = `${etype}/relationships/${rtype}/available-targets`
    }
    searchParams.append('rtype', rtype)
    if (q !== null) {
        searchParams.append('q', q)
    }
    each(params, (value, key) => searchParams.append(key, value))
    return jsonFetchCollection(`${url}?${searchParams.toString()}`)
}

function getAuthorityToGroup(eid, q = null) {
    let url = null
    url = `/fa/authority/${eid}/group_candidates`
    const searchParams = new URLSearchParams()
    if (q !== null) {
        searchParams.append('q', q)
    }
    return jsonFetchCollection(`${url}?${searchParams.toString()}`)
}

function getEntity(etype, eid) {
    const url = `/${etype}/${eid}`
    return jsonFetch(url)
}

function createEntity(etype, attributes, schemaType = null, ...files) {
    let url = `/${etype}/`
    if (schemaType !== null) {
        url = `${url}?schema_type=${schemaType}`
    }
    const headers = {}
    const attrs = JSON.stringify(attributes)
    let body
    if (files.length === 0) {
        headers['Content-Type'] = 'application/json'
        body = attrs
    } else {
        body = new FormData()
        body.append('data', attrs)
        files.forEach(([rtype, file]) => body.append(rtype, file))
    }
    const options = {
        method: 'POST',
        headers: headers,
        body: body,
    }
    return jsonFetch(url, options)
}

function relateEntity(etype, eid, rtype, attributes, targetType = null) {
    let url = `/${etype}/${eid}/relationships/${rtype}`
    const options = {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
    }
    if (targetType !== null) {
        url += `?target_type=${targetType}`
    }
    if (attributes !== undefined) {
        options.body = JSON.stringify(attributes)
    }
    return jsonFetch(url, options)
}

function addRelation(etype, eid, rtype, attributes, targetType = null) {
    let url = `/${etype}/${eid}/relationships/${rtype}/targets`
    const options = {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
    }
    if (targetType !== null) {
        url += `?target_type=${targetType}`
    }
    if (attributes !== undefined) {
        options.body = JSON.stringify(attributes)
    }
    return jsonFetch(url, options)
}

function deleteRelation(etype, eid, rtype) {
    const url = `/${etype}/${eid}/relationships/${rtype}`
    const options = {
        method: 'DELETE',
        headers: {
            'Content-Type': 'application/json',
        },
    }
    return jsonFetch(url, options)
}

function updateEntity(etype, eid, attributes) {
    const url = `/${etype}/${eid}`
    const options = {
        method: 'PUT',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(attributes),
    }
    return jsonFetch(url, options)
}

function deleteEntity(etype, eid) {
    const url = `/${etype}/${eid}`
    return jsonDeleteFetch(url)
}

function getTransitionsSchema(etype, eid) {
    const url = `/${etype}/${eid}/transitions/schema?role=creation`
    const options = {
        headers: {
            Accept: 'application/schema+json',
        },
    }
    return jsonFetch(url, options)
}

function addTransition(etype, eid, attributes) {
    const url = `/${etype}/${eid}/transitions`
    const options = {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(attributes),
    }
    return jsonFetch(url, options)
}

export default {
    buildUrl,
    jsonFetch,
    jsonSchemaFetch,
    jsonFetchCollection,
    deleteEntity,
    getSchema,
    getUiSchema,
    getRelatedSchema,
    getRelatedUiSchema,
    getEntities,
    getRelated,
    getEntity,
    createEntity,
    relateEntity,
    updateEntity,
    getTransitionsSchema,
    getAvailableTargets,
    addTransition,
    addRelation,
    deleteRelation,
    getAuthorityToGroup,
}
