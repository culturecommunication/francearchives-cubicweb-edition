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
import {BootstrapTable, TableHeaderColumn} from 'react-bootstrap-table'

import Api from '../api'

function Spinner() {
    return <i className="fa fa-spin fa-spinner"></i>
}

function IndexLabelFmt(cell, index) {
    return <a href={index.authorityUrl}>{index.label}</a>
}

function AuthorityActions(cell, authority, onNewAuthClick) {
    return (
        <button
            title="créer une autre autorité pour cet index"
            onClick={() => onNewAuthClick(authority.eid)}
        >
            <i className="fa fa-object-ungroup"></i>
        </button>
    )
}

function AuthorityTable({data, onNewAuthClick}) {
    return (
        <BootstrapTable data={data} search hover striped pagination>
            <TableHeaderColumn dataField="eid" hidden isKey>
                eid
            </TableHeaderColumn>
            <TableHeaderColumn
                width="10%"
                formatExtraData={onNewAuthClick}
                dataFormat={AuthorityActions}
            >
                Actions
            </TableHeaderColumn>
            <TableHeaderColumn width="15%" dataField="type">
                Type
            </TableHeaderColumn>
            <TableHeaderColumn
                width="45%"
                dataField="label"
                dataFormat={IndexLabelFmt}
            >
                Libéllé
            </TableHeaderColumn>
        </BootstrapTable>
    )
}

AuthorityTable.propTypes = {
    data: PropTypes.array.isRequired,
    onNewAuthClick: PropTypes.func.isRequired,
}

class AuthorityForm extends React.Component {
    constructor(props) {
        super(props)
        this.state = {submitting: false}
        this.onSubmit = this.onSubmit.bind(this)
        this.onAdd = this.onAdd.bind(this)
        this.onDelete = this.onDelete.bind(this)
    }

    onAdd() {
        this.props.onAdd(this.props.data.eid)
    }

    onDelete(idx) {
        const {data} = this.props
        this.props.onSubmit(data.eid, data.alignments[idx])
        this.setState({submitting: true})
    }

    onSubmit(ev) {
        ev.preventDefault()
        const input = ev.target.elements[0],
            idx = input.dataset.alignidx,
            {data} = this.props
        this.props.onSubmit(data.eid, data.alignments[idx], input.value)
        this.setState({submitting: true})
    }

    render() {
        const {data} = this.props,
            {submitting} = this.state
        return (
            <div>
                <h2 className="inline">
                    {' '}
                    Alignements pour &ldquo;{data.preflabel}&rdquo;{' '}
                </h2>
                <button className="btn" onClick={this.onAdd}>
                    <i className="fa fa-plus"></i> ajouter un alignement
                </button>
                <ul>
                    {data.alignments.map((alignment, idx) => {
                        return (
                            <li key={idx}>
                                <form
                                    className="form-inline"
                                    onSubmit={this.onSubmit}
                                >
                                    <input
                                        data-alignidx={idx}
                                        className="alignment-input form-control"
                                        defaultValue={alignment}
                                    />
                                    <button
                                        disabled={submitting}
                                        className="btn btn-danger"
                                        onClick={() => this.onDelete(idx)}
                                    >
                                        {submitting ? (
                                            <Spinner />
                                        ) : (
                                            <i className="fa fa-times"></i>
                                        )}
                                    </button>
                                    <button
                                        disabled={submitting}
                                        className="btn btn-primary"
                                        type="submit"
                                        value="valider"
                                    >
                                        {submitting ? <Spinner /> : null}
                                        valider
                                    </button>
                                </form>
                            </li>
                        )
                    })}
                </ul>
            </div>
        )
    }
}

AuthorityForm.propTypes = {
    data: PropTypes.object,
    onSubmit: PropTypes.func,
    onAdd: PropTypes.func,
}

// we should use reactDOM.createPortal but it is only available in react@16
function showOverlay() {
    const div = document.createElement('div')
    div.innerHTML = `
    <div class="overlay">
      <div class="overlay_background"></div>
      <div class="overlay_body">
        <i class="fa fa-spin fa-circle-o-notch"></i>
      </div>
    </div>
    `
    document.body.appendChild(div.children[0])
}

class EditAuthorityComp extends React.Component {
    constructor(props) {
        super(props)
        this.state = {loading: true, data: null}
        this.onNewAuthClick = this.onNewAuthClick.bind(this)
    }

    componentDidMount() {
        const {entity} = this.props,
            eid = entity.get('eid'),
            etype = entity.get('cw_etype')
        Api.jsonFetch(`/fa/${etype}/${eid}/indexes`).then((data) =>
            this.setState({data, loading: false}),
        )
    }

    onNewAuthClick(indexEid) {
        showOverlay()
        Api.jsonFetch(`/fa/index/${indexEid}/authority`, {method: 'POST'})
            .then((data) => document.location.replace(data.location))
            .catch((err) => {
                alert('une erreur est survenue :s')
                console.error(err)
            })
    }

    render() {
        const {loading, data} = this.state
        const title = 'Édition des index'
        if (loading) {
            return (
                <div>
                    <h1>{title}</h1>
                    <Spinner />
                </div>
            )
        }
        return (
            <div>
                <h1>{title}</h1>
                <AuthorityTable
                    data={data}
                    onNewAuthClick={this.onNewAuthClick}
                />
            </div>
        )
    }
}
EditAuthorityComp.propTypes = {
    entity: PropTypes.object.isRequired,
}

export const EditAuthority = connect(function mapStateToProps(state) {
    return {entity: state.getIn(['model', 'entity'])}
})(EditAuthorityComp)
