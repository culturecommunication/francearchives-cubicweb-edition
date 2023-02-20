/*
 * Copyright © LOGILAB S.A. (Paris, FRANCE) 2016-2020
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
import {each} from 'lodash/collection'

const {v4: uuidv4} = require('uuid')
const {Link} = require('react-router-dom')

const PropTypes = require('prop-types')

const {
    default: {buildUrl},
} = require('../api')

function setupDropDownCollapse() {
    window.onclick = function (event) {
        if (!event.target.matches('.dropbtn')) {
            const dropDowns =
                document.getElementsByClassName('dropdown-content')
            Array.from(dropDowns).forEach((dropDown) => {
                if (dropDown.classList.contains('show')) {
                    dropDown.classList.remove('show')
                }
            })
        }
    }
}

TranslationsDropDown.propTypes = {
    label: PropTypes.string.isRequired,
    url: PropTypes.string.isRequired,
    options: PropTypes.object.isRequired,
    pathes: PropTypes.object.isRequired,
    onLinkClick: PropTypes.object.isRequired,
}

export function TranslationsDropDown(props) {
    const identifier = uuidv4(),
        {label, url, options, pathes, onLinkClick} = props,
        showFunction = () =>
            document.getElementById(identifier).classList.toggle('show')
    let links = []
    each(options, (key, value) => {
        let restpath = pathes[value] || null
        if (restpath === null) {
            links.push(
                <Link to={{pathname: `${url}_${value}`}} onClick={onLinkClick}>
                    <span className="action">{key}</span>
                </Link>,
            )
        } else {
            links.push(<a href={buildUrl(`${restpath}#/edit`)}>{key}</a>)
        }
    })

    setupDropDownCollapse()
    return (
        <div className="dropdown">
            <h3 onClick={showFunction} className="action dropbtn">
                <i name="plus" className="fa fa-list"></i>
                {label}
            </h3>
            <div id={identifier} className="dropdown-content">
                {links}
            </div>
        </div>
    )
}
