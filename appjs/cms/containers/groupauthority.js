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
import React from 'react'
import {connect} from 'react-redux'
import PropTypes from 'prop-types'
import {throttle} from 'lodash/function'
const {Alert} = require('../components/error')
import Async from 'react-select/async'

import Api from '../api'

function Spinner() {
    return <i className="fa fa-spin fa-spinner"></i>
}

class IdEntry extends React.Component {
    render() {
        const {authtype, eid, onChange, selectedAuthority} = this.props
        let link = null
        if (selectedAuthority && selectedAuthority.value) {
            let href =
                window.BASE_URL + authtype + '/' + selectedAuthority.value
            link = (
                <div>
                    <span className="control-label col-xs-1">
                        voir l&quot;autorité séléctionnée
                    </span>
                    <a
                        href={href}
                        className="fa-stack fa-lg url_link col-xs-2"
                        target="_blank"
                        rel="noopener noreferrer"
                    >
                        <i className="fa fa-circle fa-stack-2x"></i>
                        <i className="fa fa-arrow-right fa-stack-1x fa-inverse"></i>
                    </a>
                </div>
            )
        }

        function loadOptions(input) {
            if (input.length < 3) {
                return []
            }
            return Api.getAuthorityToGroup(eid, input).then(d =>
                d.map(e => ({label: e.title, value: e.eid})),
            )
        }

        function filterOptions(options) {
            // Do no filtering, just return all options
            return options
        }
        return (
            <fieldset>
                <legend>Autorité à grouper</legend>
                <div className="form-group">
                    <label className="control-label col-xs-1">libellé</label>
                    <Async
                        cache={false}
                        className="col-xs-9"
                        name="sameas"
                        noOptionsMessage={() => 'aucune autorité trouvée'}
                        value={selectedAuthority}
                        loadOptions={throttle(loadOptions, 300)}
                        onBlurResetsInput={false}
                        isMulti={false}
                        isClearable={true}
                        placeholder="Sélectionnez une autorité par son libellé..."
                        onChange={onChange}
                        filterOptions={filterOptions}
                    />
                    {link}
                </div>
            </fieldset>
        )
    }
}
IdEntry.propTypes = {
    onChange: PropTypes.func.isRequired,
    eid: PropTypes.string.isRequired,
    authtype: PropTypes.string.isRequired,
    selectedAuthority: PropTypes.shape.isRequired,
}

function IdList({eid, authtype, ids, onIdChange}) {
    return (
        <div className="list-group">
            {ids.map((selected, idx) => (
                <div className="list-group-item" key={idx}>
                    <IdEntry
                        eid={eid}
                        authtype={authtype}
                        onChange={options => onIdChange(idx, options)}
                        selectedAuthority={selected}
                    />
                </div>
            ))}
        </div>
    )
}
IdList.propTypes = {
    ids: PropTypes.array.isRequired,
    onIdChange: PropTypes.func.isRequired,
    eid: PropTypes.string.isRequired,
    authtype: PropTypes.string.isRequired,
}

const location_reload = document.location.reload.bind(document.location)

class GroupAuthorityComp extends React.Component {
    constructor(props) {
        super(props)
        this.state = {ids: [], submitting: false, message: null}
        this.onAdd = this.onAdd.bind(this)
        this.onIdChange = this.onIdChange.bind(this)
        this.onSubmit = this.onSubmit.bind(this)
        this.onCancel = this.props.onCancel || location_reload
        this.updateMessage = msg => this.setState({message: msg})
    }

    onSubmit(ev) {
        ev.preventDefault()
        const {entity} = this.props,
            {ids} = this.state,
            eid = entity.get('eid')
        const eids = ids
            .filter(el => {
                return el !== null
            })
            .map(e => e.value)
        this.setState({submitting: true})
        Api.jsonFetch(`/fa/authority/${eid}/_group`, {
            method: 'POST',
            body: JSON.stringify(eids),
        })
            .then(() => {
                this.updateMessage({
                    type: 'success',
                    text: `${eids.length} Autorités groupées.`,
                })
                this.setState({submitting: false, ids: []})
            })
            .catch(e => {
                this.updateMessage({type: 'danger', text: e})
                console.error(e)
            })
    }

    onIdChange(idx, selectedAuthority) {
        const {ids} = this.state
        ids[idx] = selectedAuthority
        this.setState({ids})
    }

    onAdd() {
        const {ids} = this.state
        ids.unshift(null)
        this.setState({ids})
        return false
    }

    render() {
        const {entity} = this.props,
            {ids, submitting, message} = this.state,
            title = entity.get('dc_title'),
            i18netype = entity.get('i18n_cw_etype'),
            eid = entity.get('eid'),
            etype = entity.get('cw_etype')
        const isEmpty = array => !array.filter(e => e !== null).length
        return (
            <div>
                <h1>
                    {i18netype} : &quot;{title}&quot;
                </h1>
                <Alert message={message} />
                <div className="cms_add_link">
                    <button
                        title="ajouter des autorités"
                        className="btn btn-default"
                        onClick={this.onAdd}
                    >
                        Cliquer ici
                    </button>
                    &nbsp; pour ajouter des autorités à grouper
                </div>
                <form className="form-horizontal" onSubmit={this.onSubmit}>
                    <IdList
                        className="form-group field"
                        eid={eid}
                        authtype={etype}
                        ids={ids}
                        onIdChange={this.onIdChange}
                    />
                    <div className="btn-group">
                        <button
                            className="btn btn-default"
                            type="button"
                            onClick={this.onCancel}
                        >
                            fermer
                        </button>
                        <button
                            disabled={submitting || isEmpty(ids)}
                            className="btn btn-primary"
                            type="submit"
                        >
                            {submitting ? <Spinner /> : null}
                            enregistrer
                        </button>
                    </div>
                </form>
            </div>
        )
    }
}
GroupAuthorityComp.propTypes = {
    entity: PropTypes.object.isRequired,
    onCancel: PropTypes.func,
}

export const GroupAuthority = connect(function mapStateToProps(state) {
    return {entity: state.getIn(['model', 'entity'])}
})(GroupAuthorityComp)
